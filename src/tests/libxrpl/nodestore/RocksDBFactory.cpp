#include <xrpl/basics/ByteUtilities.h>

#if XRPL_ROCKSDB_AVAILABLE

#include <xrpl/basics/BasicConfig.h>
#include <xrpl/beast/utility/Journal.h>
#include <xrpl/beast/utility/temp_dir.h>
#include <xrpl/beast/xor_shift_engine.h>
#include <xrpl/nodestore/Backend.h>
#include <xrpl/nodestore/DummyScheduler.h>
#include <xrpl/nodestore/Manager.h>
#include <xrpl/nodestore/NodeObject.h>

#include <gtest/gtest.h>
#include <helpers/TestSink.h>
#include <nodestore/TestBase.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <string>

namespace xrpl::NodeStore {

namespace {

constexpr std::uint64_t kSeedValue = 72;
constexpr int kNumObjects = 200;

}  // namespace

class RocksDBFactoryTest : public ::testing::Test
{
protected:
    // A Section pre-populated with the mandatory keys; tests add tuning keys.
    [[nodiscard]] Section
    baseParams() const
    {
        Section params;
        params.set("type", "rocksdb");
        params.set("path", tempDir_.path());
        return params;
    }

    std::unique_ptr<Backend>
    makeBackend(Section const& params)
    {
        return Manager::instance().makeBackend(params, megabytes(4), scheduler_, journal_);
    }

    std::unique_ptr<Backend>
    makeOpenBackend(Section const& params)
    {
        auto backend = makeBackend(params);
        backend->open();
        return backend;
    }

    DummyScheduler scheduler_;
    beast::TempDir const tempDir_;
    beast::Journal const journal_{TestSink::instance()};
};

// The factory is registered under the case-insensitive name "RocksDB".
TEST_F(RocksDBFactoryTest, factory_registered_and_named)
{
    auto* factory = Manager::instance().find("rocksdb");
    ASSERT_NE(factory, nullptr);
    EXPECT_EQ(factory->getName(), "RocksDB");
    // case-insensitive lookup
    EXPECT_EQ(Manager::instance().find("RocksDB"), factory);
}

// A backend cannot be constructed without a path.
TEST_F(RocksDBFactoryTest, missing_path_throws)
{
    Section params;
    params.set("type", "rocksdb");
    EXPECT_THROW(makeBackend(params), std::exception);
}

// open_files adjusts the reported fd requirement (value + 128), and the
// legacy default of 2000 is bumped to 8000 unless hard_set is given.
TEST_F(RocksDBFactoryTest, open_files_adjusts_fd_required)
{
    {
        SCOPED_TRACE("legacy 2000 -> 8000 when not hard_set");
        auto params = baseParams();
        params.set("open_files", "2000");
        auto backend = makeBackend(params);
        EXPECT_EQ(backend->fdRequired(), 8000 + 128);
    }
    {
        SCOPED_TRACE("hard_set keeps 2000 verbatim");
        auto params = baseParams();
        params.set("open_files", "2000");
        params.set("hard_set", "1");
        auto backend = makeBackend(params);
        EXPECT_EQ(backend->fdRequired(), 2000 + 128);
    }
    {
        SCOPED_TRACE("non-default value passes through");
        auto params = baseParams();
        params.set("open_files", "5000");
        auto backend = makeBackend(params);
        EXPECT_EQ(backend->fdRequired(), 5000 + 128);
    }
}

// Without open_files the backend reports its built-in default.
TEST_F(RocksDBFactoryTest, default_fd_required)
{
    auto backend = makeBackend(baseParams());
    EXPECT_EQ(backend->fdRequired(), 2048);
}

// Exercises every tuning-option branch of the constructor (soft defaults path).
TEST_F(RocksDBFactoryTest, tuning_options_parse_soft_defaults)
{
    auto params = baseParams();
    params.set("cache_mb", "256");  // remapped to 1024 (not hard_set)
    params.set("filter_bits", "12");
    params.set("filter_full", "0");   // -> block-level bloom filter
    params.set("file_size_mb", "8");  // remapped to 256 (not hard_set)
    params.set("file_size_mult", "2");
    params.set("bg_threads", "2");
    params.set("high_threads", "2");  // >0 also sets background flushes
    params.set("block_size", "8192");
    params.set("universal_compaction", "1");

    auto backend = makeBackend(params);
    EXPECT_NE(backend, nullptr);
    EXPECT_FALSE(backend->getBlockSize().has_value());  // RocksDB doesn't report one
}

// hard_set path: magic default values are taken verbatim (no remap).
TEST_F(RocksDBFactoryTest, tuning_options_parse_hard_set)
{
    auto params = baseParams();
    params.set("hard_set", "1");
    params.set("cache_mb", "256");
    params.set("filter_bits", "10");
    params.set("filter_full", "1");  // -> full bloom filter
    params.set("file_size_mb", "8");
    params.set("high_threads", "0");          // not >0: skips flush tweak
    params.set("universal_compaction", "0");  // present but disabled

    auto backend = makeBackend(params);
    EXPECT_NE(backend, nullptr);
}

// bbt_options string is parsed; bad input throws.
TEST_F(RocksDBFactoryTest, bbt_options_valid_and_invalid)
{
    {
        auto params = baseParams();
        params.set("bbt_options", "block_size=4096");
        EXPECT_NO_THROW(makeBackend(params));
    }
    {
        auto params = baseParams();
        params.set("bbt_options", "not_a_real_option=1");
        EXPECT_THROW(makeBackend(params), std::exception);
    }
}

// options string is parsed; bad input throws.
TEST_F(RocksDBFactoryTest, options_valid_and_invalid)
{
    {
        auto params = baseParams();
        params.set("options", "max_open_files=500");
        EXPECT_NO_THROW(makeBackend(params));
    }
    {
        auto params = baseParams();
        params.set("options", "not_a_real_option=1");
        EXPECT_THROW(makeBackend(params), std::exception);
    }
}

// Opening a non-existent database without createIfMissing fails.
TEST_F(RocksDBFactoryTest, open_without_create_throws)
{
    auto backend = makeBackend(baseParams());
    EXPECT_FALSE(backend->isOpen());
    EXPECT_THROW(backend->open(false), std::exception);
}

// Full store/fetch round-trip through the batched write path.
TEST_F(RocksDBFactoryTest, store_fetch_roundtrip)
{
    auto backend = makeOpenBackend(baseParams());
    EXPECT_TRUE(backend->isOpen());
    EXPECT_EQ(backend->getName(), tempDir_.path());

    beast::xor_shift_engine rng(kSeedValue);
    auto batch = createPredictableBatch(kNumObjects, rng());
    storeBatch(*backend, batch);

    {
        SCOPED_TRACE("original order");
        auto const copy = fetchCopyOfBatch(*backend, batch);
        EXPECT_TRUE(areBatchesEqual(batch, copy));
    }
    {
        SCOPED_TRACE("shuffled order");
        std::shuffle(batch.begin(), batch.end(), rng);
        auto const copy = fetchCopyOfBatch(*backend, batch);
        EXPECT_TRUE(areBatchesEqual(batch, copy));
    }

    EXPECT_GE(backend->getWriteLoad(), 0);
}

// Fetching an absent key reports NotFound.
TEST_F(RocksDBFactoryTest, fetch_missing_returns_notfound)
{
    auto backend = makeOpenBackend(baseParams());
    beast::xor_shift_engine rng(kSeedValue + 1);
    auto const batch = createPredictableBatch(16, rng());
    fetchMissing(*backend, batch);
}

// forEach visits every stored object exactly once.
TEST_F(RocksDBFactoryTest, for_each_visits_all)
{
    auto backend = makeOpenBackend(baseParams());
    beast::xor_shift_engine rng(kSeedValue + 2);
    auto const batch = createPredictableBatch(kNumObjects, rng());
    storeBatch(*backend, batch);

    std::size_t count = 0;
    backend->forEach([&count](std::shared_ptr<NodeObject> obj) {
        EXPECT_NE(obj, nullptr);
        ++count;
    });
    EXPECT_EQ(count, batch.size());
}

// setDeletePath() causes the database directory to be removed on close.
TEST_F(RocksDBFactoryTest, set_delete_path_removes_files)
{
    beast::TempDir const dir;
    Section params;
    params.set("type", "rocksdb");
    params.set("path", dir.path());

    auto backend = makeBackend(params);
    backend->open();
    beast::xor_shift_engine rng(kSeedValue + 3);
    auto const batch = createPredictableBatch(8, rng());
    storeBatch(*backend, batch);

    backend->setDeletePath();
    backend->close();

    EXPECT_FALSE(std::filesystem::exists(dir.path()));
}

}  // namespace xrpl::NodeStore

#endif  // XRPL_ROCKSDB_AVAILABLE
