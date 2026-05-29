#include <xrpl/nodestore/detail/codec.h>

#include <xrpl/nodestore/detail/varint.h>
#include <xrpl/protocol/HashPrefix.h>

#include <gtest/gtest.h>
#include <nudb/detail/stream.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace xrpl::NodeStore {

namespace {

// A minimal BufferFactory for the codec functions: invoking it with a size
// returns a pointer to owned storage of that size. The instance must outlive
// any use of the returned pointer. The codec invokes its BufferFactory at most
// once per call, so a single resize is sufficient.
class Buffer
{
    std::vector<std::uint8_t> data_;

public:
    void*
    operator()(std::size_t n)
    {
        data_.resize(n);
        return data_.data();
    }
};

// An inner node is a 525-byte blob laid out as:
//   [index:u32][unused:u32][kind:u8][prefix:u32 == HashPrefix::InnerNode]
//   [16 child hashes, 32 bytes each]
// Build it with the same nudb stream primitives the codec uses so the field
// encoding (byte order) matches exactly. `present` selects which of the 16
// child slots are non-zero; absent slots stay zeroed.
constexpr std::size_t kInnerNodeSize = 525;

std::vector<std::uint8_t>
makeInnerNode(std::array<bool, 16> const& present)
{
    using namespace nudb::detail;

    std::vector<std::uint8_t> v(kInnerNodeSize, 0);
    ostream os(v.data(), v.size());
    // Non-zero header values so filterInner has something to erase.
    write<std::uint32_t>(os, 0x12345678);  // index (ledger sequence)
    write<std::uint32_t>(os, 0x9abcdef0);  // unused
    write<std::uint8_t>(os, 0x07);         // kind (node object type)
    write<std::uint32_t>(os, static_cast<std::uint32_t>(HashPrefix::InnerNode));
    for (int i = 0; i < 16; ++i)
    {
        std::array<std::uint8_t, 32> h{};
        if (present[i])
        {
            for (int j = 0; j < 32; ++j)
                h[j] = static_cast<std::uint8_t>((((i * 32) + j) % 251) + 1);
        }
        write(os, h.data(), h.size());
    }
    return v;
}

// First byte of a compressed blob is the codec type, encoded as a varint.
std::uint8_t
codecType(std::pair<void const*, std::size_t> const& blob)
{
    std::size_t type = 0;
    readVarint(blob.first, blob.second, type);
    return static_cast<std::uint8_t>(type);
}

}  // namespace

//------------------------------------------------------------------------------
// lz4Compress / lz4Decompress
//------------------------------------------------------------------------------

TEST(codec, lz4_round_trip)
{
    std::vector<std::string> const kBlobs = {
        std::string(1, '\0'),
        "hello world",
        std::string(256, 'A'),        // highly compressible
        std::string(64 * 1024, 'z'),  // large + compressible
    };

    for (auto const& blob : kBlobs)
    {
        SCOPED_TRACE("size=" + std::to_string(blob.size()));
        Buffer cbuf;
        auto const comp = lz4Compress(blob.data(), blob.size(), cbuf);
        Buffer dbuf;
        auto const dec = lz4Decompress(comp.first, comp.second, dbuf);
        ASSERT_EQ(dec.second, blob.size());
        EXPECT_EQ(std::memcmp(dec.first, blob.data(), blob.size()), 0);
    }
}

TEST(codec, lz4_decompress_invalid_blob)
{
    // A complete varint with no compressed payload following it: readVarint
    // consumes the whole input, leaving nothing to decompress.
    std::array<std::uint8_t, 1> const bad{0x05};
    Buffer buf;
    EXPECT_THROW(lz4Decompress(bad.data(), bad.size(), buf), std::runtime_error);
}

//------------------------------------------------------------------------------
// nodeobjectCompress / nodeobjectDecompress
//------------------------------------------------------------------------------

TEST(codec, nodeobject_lz4_round_trip)
{
    // Generic (non-inner-node) payloads take the type 1 (lz4) path.
    std::vector<std::string> const kBlobs = {
        "a small node object payload",
        std::string(1000, 'q'),
        std::string(kInnerNodeSize, 'x'),  // 525 bytes but no InnerNode prefix
    };

    for (auto const& blob : kBlobs)
    {
        SCOPED_TRACE("size=" + std::to_string(blob.size()));
        Buffer cbuf;
        auto const comp = nodeobjectCompress(blob.data(), blob.size(), cbuf);
        EXPECT_EQ(codecType(comp), 1) << "expected lz4 codec type";
        Buffer dbuf;
        auto const dec = nodeobjectDecompress(comp.first, comp.second, dbuf);
        ASSERT_EQ(dec.second, blob.size());
        EXPECT_EQ(std::memcmp(dec.first, blob.data(), blob.size()), 0);
    }
}

TEST(codec, nodeobject_decompress_uncompressed)
{
    // Manually frame a type 0 (uncompressed) blob: [varint(0)][raw bytes].
    std::string const raw = "uncompressed payload";
    std::vector<std::uint8_t> framed;
    framed.push_back(0x00);  // varint for type 0
    framed.insert(framed.end(), raw.begin(), raw.end());

    Buffer buf;
    auto const dec = nodeobjectDecompress(framed.data(), framed.size(), buf);
    ASSERT_EQ(dec.second, raw.size());
    EXPECT_EQ(std::memcmp(dec.first, raw.data(), raw.size()), 0);
}

TEST(codec, inner_node_compressed_round_trip)
{
    // Fewer than 16 non-zero children -> type 2 (compressed inner node).
    std::vector<std::array<bool, 16>> const kCases = {
        // single child
        {{true,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
          false}},
        // a scattered handful
        {{true,
          false,
          true,
          false,
          false,
          true,
          false,
          false,
          false,
          true,
          false,
          false,
          false,
          false,
          false,
          true}},
        // 15 of 16 (boundary just below the full-node threshold)
        {{true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          false}},
    };

    for (std::size_t c = 0; c < kCases.size(); ++c)
    {
        SCOPED_TRACE("case=" + std::to_string(c));
        auto node = makeInnerNode(kCases[c]);
        // The codec only round-trips the filtered form (ledger seq / type erased).
        filterInner(node.data(), node.size());

        Buffer cbuf;
        auto const comp = nodeobjectCompress(node.data(), node.size(), cbuf);
        EXPECT_EQ(codecType(comp), 2) << "expected compressed inner node";

        Buffer dbuf;
        auto const dec = nodeobjectDecompress(comp.first, comp.second, dbuf);
        ASSERT_EQ(dec.second, node.size());
        EXPECT_EQ(std::memcmp(dec.first, node.data(), node.size()), 0);
    }
}

TEST(codec, inner_node_full_round_trip)
{
    // All 16 children non-zero -> type 3 (full inner node).
    std::array<bool, 16> all{};
    all.fill(true);
    auto node = makeInnerNode(all);
    filterInner(node.data(), node.size());

    Buffer cbuf;
    auto const comp = nodeobjectCompress(node.data(), node.size(), cbuf);
    EXPECT_EQ(codecType(comp), 3) << "expected full inner node";

    Buffer dbuf;
    auto const dec = nodeobjectDecompress(comp.first, comp.second, dbuf);
    ASSERT_EQ(dec.second, node.size());
    EXPECT_EQ(std::memcmp(dec.first, node.data(), node.size()), 0);
}

TEST(codec, nodeobject_decompress_empty)
{
    // Empty input: the leading type varint cannot be read.
    std::array<std::uint8_t, 1> const dummy{0x00};
    Buffer buf;
    EXPECT_THROW(nodeobjectDecompress(dummy.data(), 0, buf), std::runtime_error);
}

TEST(codec, nodeobject_decompress_bad_type)
{
    // Type byte 4 is not a defined codec type.
    std::array<std::uint8_t, 1> bad{};
    auto const n = writeVarint(bad.data(), 4);
    Buffer buf;
    EXPECT_THROW(nodeobjectDecompress(bad.data(), n, buf), std::runtime_error);
}

//------------------------------------------------------------------------------
// filterInner
//------------------------------------------------------------------------------

TEST(codec, filter_inner_erases_header)
{
    std::array<bool, 16> some{};
    some[0] = some[3] = true;
    auto node = makeInnerNode(some);

    filterInner(node.data(), node.size());

    // index (4) + unused (4) + kind (1) are zeroed; the prefix and hashes remain.
    for (std::size_t i = 0; i < 9; ++i)
        EXPECT_EQ(node[i], 0u) << "byte " << i << " not erased";

    using namespace nudb::detail;
    istream is(node.data(), node.size());
    std::uint32_t index = 0, unused = 0, prefix = 0;
    std::uint8_t kind = 0;
    read<std::uint32_t>(is, index);
    read<std::uint32_t>(is, unused);
    read<std::uint8_t>(is, kind);
    read<std::uint32_t>(is, prefix);
    EXPECT_EQ(prefix, static_cast<std::uint32_t>(HashPrefix::InnerNode));
}

TEST(codec, filter_inner_ignores_non_inner)
{
    // Wrong size: must be left untouched.
    std::vector<std::uint8_t> blob(100, 0xab);
    auto const copy = blob;
    filterInner(blob.data(), blob.size());
    EXPECT_EQ(blob, copy);

    // Correct size but prefix is not InnerNode: also untouched.
    std::vector<std::uint8_t> notInner(kInnerNodeSize, 0xcd);
    auto const notInnerCopy = notInner;
    filterInner(notInner.data(), notInner.size());
    EXPECT_EQ(notInner, notInnerCopy);
}

//------------------------------------------------------------------------------
// zero32
//------------------------------------------------------------------------------

TEST(codec, zero32_is_zeroed)
{
    std::array<std::uint8_t, 32> const zeros{};
    EXPECT_EQ(std::memcmp(zero32(), zeros.data(), 32), 0);
}

}  // namespace xrpl::NodeStore
