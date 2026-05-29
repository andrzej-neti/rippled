#include <xrpl/nodestore/Database.h>

#include <xrpl/basics/BasicConfig.h>
#include <xrpl/basics/ByteUtilities.h>
#include <xrpl/basics/base_uint.h>
#include <xrpl/beast/utility/Journal.h>
#include <xrpl/beast/utility/temp_dir.h>
#include <xrpl/beast/xor_shift_engine.h>
#include <xrpl/json/json_value.h>
#include <xrpl/nodestore/Backend.h>
#include <xrpl/nodestore/DummyScheduler.h>
#include <xrpl/nodestore/Manager.h>
#include <xrpl/nodestore/NodeObject.h>
#include <xrpl/nodestore/Types.h>
#include <xrpl/nodestore/detail/DatabaseNodeImp.h>
#include <xrpl/protocol/SystemParameters.h>
#include <xrpl/protocol/jss.h>

#include <gtest/gtest.h>
#include <helpers/TestSink.h>
#include <nodestore/TestBase.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace xrpl::NodeStore {

namespace {

constexpr std::int64_t kSeedValue = 50;
constexpr int kNumObjects = 2000;

std::vector<std::string>
allBackends()
{
    std::vector<std::string> types{"memory", "nudb"};
#if XRPL_ROCKSDB_AVAILABLE
    types.emplace_back("rocksdb");
#endif
    return types;
}

std::vector<std::string>
persistentBackends()
{
    std::vector<std::string> types{"nudb"};
#if XRPL_ROCKSDB_AVAILABLE
    types.emplace_back("rocksdb");
#endif
    return types;
}

std::vector<std::string>
importBackends()
{
    std::vector<std::string> types{"nudb"};
#if XRPL_ROCKSDB_AVAILABLE
    types.emplace_back("rocksdb");
#endif
#ifdef XRPL_ENABLE_SQLITE_BACKEND_TESTS
    types.emplace_back("sqlite");
#endif
    return types;
}

// A controllable Backend used to drive DatabaseNodeImp::fetchNodeObject down
// each of its status branches without needing on-disk corruption.
class StubBackend : public Backend
{
public:
    enum class Mode { Ok, NotFound, Corrupt, Unknown, Throwing };

    Mode mode{Mode::Ok};
    std::shared_ptr<NodeObject> object;

    std::string
    getName() override
    {
        return "stub";
    }
    void
    open(bool) override
    {
    }
    bool
    isOpen() override
    {
        return true;
    }
    void
    close() override
    {
    }
    Status
    fetch(uint256 const&, std::shared_ptr<NodeObject>* pObject) override
    {
        switch (mode)
        {
            case Mode::Ok:
                *pObject = object;
                return Status::Ok;
            case Mode::NotFound:
                return Status::NotFound;
            case Mode::Corrupt:
                return Status::DataCorrupt;
            case Mode::Unknown:
                return static_cast<Status>(99);  // not a known Status value
            case Mode::Throwing:
                throw std::runtime_error("stub fetch failure");
        }
        return Status::Ok;
    }
    void
    store(std::shared_ptr<NodeObject> const&) override
    {
    }
    void
    storeBatch(Batch const&) override
    {
    }
    void
    sync() override
    {
    }
    void
    forEach(std::function<void(std::shared_ptr<NodeObject>)>) override
    {
    }
    int
    getWriteLoad() override
    {
        return 0;
    }
    void
    setDeletePath() override
    {
    }
    [[nodiscard]] int
    fdRequired() const override
    {
        return 1;
    }
};

}  // namespace

class NodeStoreDatabaseTest : public ::testing::TestWithParam<std::string>
{
};

class NodeStoreDatabasePersistenceTest : public ::testing::TestWithParam<std::string>
{
};

TEST_P(NodeStoreDatabaseTest, StoreAndFetch)
{
    auto const type = GetParam();

    DummyScheduler scheduler;
    beast::TempDir const nodeDb;
    Section nodeParams;
    nodeParams.set("type", type);
    nodeParams.set("path", nodeDb.path());

    beast::Journal const journal(TestSink::instance());
    beast::xor_shift_engine rng(kSeedValue);
    auto batch = createPredictableBatch(kNumObjects, rng());

    auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);

    storeBatch(*db, batch);

    {
        SCOPED_TRACE("read in original order");
        auto const copy = fetchCopyOfBatch(*db, batch);
        EXPECT_TRUE(areBatchesEqual(batch, copy));
    }

    {
        SCOPED_TRACE("read in shuffled order");
        std::shuffle(batch.begin(), batch.end(), rng);
        auto const copy = fetchCopyOfBatch(*db, batch);
        EXPECT_TRUE(areBatchesEqual(batch, copy));
    }
}

TEST_P(NodeStoreDatabasePersistenceTest, RoundTrip)
{
    auto const type = GetParam();

    DummyScheduler scheduler;
    beast::TempDir const nodeDb;
    Section nodeParams;
    nodeParams.set("type", type);
    nodeParams.set("path", nodeDb.path());

    beast::Journal const journal(TestSink::instance());
    beast::xor_shift_engine rng(kSeedValue);
    auto batch = createPredictableBatch(kNumObjects, rng());

    {
        auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
        storeBatch(*db, batch);
    }

    // re-open without the ephemeral db
    auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);

    auto copy = fetchCopyOfBatch(*db, batch);
    std::ranges::sort(batch, LessThan{});
    std::ranges::sort(copy, LessThan{});
    EXPECT_TRUE(areBatchesEqual(batch, copy));
}

INSTANTIATE_TEST_SUITE_P(
    NodeStoreBackends,
    NodeStoreDatabaseTest,
    ::testing::ValuesIn(allBackends()),
    [](::testing::TestParamInfo<std::string> const& info) { return info.param; });

INSTANTIATE_TEST_SUITE_P(
    PersistentBackends,
    NodeStoreDatabasePersistenceTest,
    ::testing::ValuesIn(persistentBackends()),
    [](::testing::TestParamInfo<std::string> const& info) { return info.param; });

TEST(NodeStoreDatabase, MemoryEarliestSeq)
{
    DummyScheduler scheduler;
    beast::TempDir const nodeDb;
    Section nodeParams;
    nodeParams.set("type", "memory");
    nodeParams.set("path", nodeDb.path());

    beast::Journal const journal(TestSink::instance());

    // default earliest ledger sequence
    {
        auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
        EXPECT_EQ(db->earliestLedgerSeq(), kXrpLedgerEarliestSeq);
    }

    // invalid earliest_seq value
    {
        nodeParams.set("earliest_seq", "0");
        try
        {
            auto db =
                Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
            FAIL() << "expected runtime_error for earliest_seq=0";
        }
        catch (std::runtime_error const& e)
        {
            EXPECT_STREQ(e.what(), "Invalid earliest_seq");
        }
    }

    // valid earliest_seq value
    {
        nodeParams.set("earliest_seq", "1");
        auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
        EXPECT_EQ(db->earliestLedgerSeq(), 1u);
    }
}

class DatabaseImportTest : public ::testing::TestWithParam<std::string>
{
};

TEST_P(DatabaseImportTest, SameBackend)
{
    auto const type = GetParam();

    DummyScheduler scheduler;
    beast::Journal const journal(TestSink::instance());

    beast::TempDir const srcDir;
    Section srcParams;
    srcParams.set("type", type);
    srcParams.set("path", srcDir.path());

    auto batch = createPredictableBatch(kNumObjects, kSeedValue);

    // write to source db
    {
        auto src = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, srcParams, journal);
        storeBatch(*src, batch);
    }

    Batch copy;
    {
        // re-open source and import into a fresh destination
        auto src = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, srcParams, journal);

        beast::TempDir const destDir;
        Section destParams;
        destParams.set("type", type);
        destParams.set("path", destDir.path());

        auto dest =
            Manager::instance().makeDatabase(megabytes(4), scheduler, 2, destParams, journal);

        dest->importDatabase(*src);
        copy = fetchCopyOfBatch(*dest, batch);
    }

    std::ranges::sort(batch, LessThan{});
    std::ranges::sort(copy, LessThan{});
    EXPECT_TRUE(areBatchesEqual(batch, copy));
}

INSTANTIATE_TEST_SUITE_P(
    ImportBackends,
    DatabaseImportTest,
    ::testing::ValuesIn(importBackends()),
    [](::testing::TestParamInfo<std::string> const& info) { return info.param; });

// Drive DatabaseNodeImp's fetch status handling and trivial accessors directly,
// using a stub backend to reach branches that a healthy on-disk store can't.
TEST(NodeStoreDatabaseNode, fetch_status_branches)
{
    DummyScheduler scheduler;
    beast::Journal const journal(TestSink::instance());
    Section const config;  // empty -> defaults

    auto stub = std::make_shared<StubBackend>();
    auto* stubPtr = stub.get();

    DatabaseNodeImp db(scheduler, 1, std::move(stub), config, journal);
    // The public fetchNodeObject overload lives on Database; the concrete type's
    // private override otherwise hides it, so drive fetches through a base ref.
    Database& base = db;

    // Trivial accessors (delegate to the backend).
    EXPECT_EQ(base.getName(), "stub");
    EXPECT_EQ(base.getWriteLoad(), 0);
    EXPECT_TRUE(base.isSameDB(1, 2));
    EXPECT_NO_THROW(base.sync());

    auto const batch = createPredictableBatch(4, 1234);
    auto const& h = batch[0]->getHash();

    {
        SCOPED_TRACE("Ok returns the object");
        stubPtr->mode = StubBackend::Mode::Ok;
        stubPtr->object = batch[0];
        auto const obj = base.fetchNodeObject(h);
        ASSERT_NE(obj, nullptr);
        EXPECT_EQ(obj->getHash(), h);
    }
    {
        SCOPED_TRACE("NotFound returns nullptr");
        stubPtr->mode = StubBackend::Mode::NotFound;
        EXPECT_EQ(base.fetchNodeObject(batch[1]->getHash()), nullptr);
    }
    {
        SCOPED_TRACE("DataCorrupt returns nullptr");
        stubPtr->mode = StubBackend::Mode::Corrupt;
        EXPECT_EQ(base.fetchNodeObject(batch[2]->getHash()), nullptr);
    }
    {
        SCOPED_TRACE("unknown status returns nullptr");
        stubPtr->mode = StubBackend::Mode::Unknown;
        EXPECT_EQ(base.fetchNodeObject(batch[3]->getHash()), nullptr);
    }
    {
        SCOPED_TRACE("backend exception is rethrown");
        stubPtr->mode = StubBackend::Mode::Throwing;
        EXPECT_THROW(base.fetchNodeObject(h), std::exception);
    }
}

namespace {

// Issue a single asyncFetch and block until the read-thread fires the callback
// (or a generous timeout elapses). The promise is heap-owned so it outlives the
// callback even if this helper returns early on timeout.
std::shared_ptr<NodeObject>
asyncFetchBlocking(Database& db, uint256 const& hash, std::uint32_t ledgerSeq)
{
    auto promise = std::make_shared<std::promise<std::shared_ptr<NodeObject>>>();
    auto future = promise->get_future();

    db.asyncFetch(hash, ledgerSeq, [promise](std::shared_ptr<NodeObject> const& obj) {
        promise->set_value(obj);
    });

    if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
        return nullptr;
    return future.get();
}

}  // namespace

// Drive the asynchronous read path: asyncFetch enqueues a request that a read
// thread services via fetchNodeObject, then invokes the supplied callback.
TEST(NodeStoreDatabaseAsync, FetchStoredObject)
{
    DummyScheduler scheduler;
    beast::TempDir const nodeDb;
    Section nodeParams;
    nodeParams.set("type", "memory");
    nodeParams.set("path", nodeDb.path());

    beast::Journal const journal(TestSink::instance());
    auto const batch = createPredictableBatch(8, kSeedValue);

    auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
    storeBatch(*db, batch);

    for (auto const& obj : batch)
    {
        SCOPED_TRACE("async fetch of stored object");
        auto const fetched = asyncFetchBlocking(*db, obj->getHash(), db->earliestLedgerSeq());
        ASSERT_NE(fetched, nullptr);
        EXPECT_TRUE(isSame(fetched, obj));
    }
}

// A request for a hash that is not present resolves the callback with nullptr.
TEST(NodeStoreDatabaseAsync, FetchMissingObject)
{
    DummyScheduler scheduler;
    beast::TempDir const nodeDb;
    Section nodeParams;
    nodeParams.set("type", "memory");
    nodeParams.set("path", nodeDb.path());

    beast::Journal const journal(TestSink::instance());
    auto const batch = createPredictableBatch(1, kSeedValue);

    auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
    // nothing stored

    auto promise = std::make_shared<std::promise<std::shared_ptr<NodeObject>>>();
    auto future = promise->get_future();
    bool called = false;
    db->asyncFetch(
        batch[0]->getHash(),
        db->earliestLedgerSeq(),
        [promise, &called](std::shared_ptr<NodeObject> const& obj) {
            called = true;
            promise->set_value(obj);
        });

    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(called);
    EXPECT_EQ(future.get(), nullptr);
}

// getCountsJson reports store/fetch statistics into a JSON object.
TEST(NodeStoreDatabaseCounts, GetCountsJson)
{
    DummyScheduler scheduler;
    beast::TempDir const nodeDb;
    Section nodeParams;
    nodeParams.set("type", "memory");
    nodeParams.set("path", nodeDb.path());

    beast::Journal const journal(TestSink::instance());
    auto const batch = createPredictableBatch(4, kSeedValue);

    auto db = Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
    storeBatch(*db, batch);

    // A couple of synchronous fetches so the read counters are non-zero.
    auto const copy = fetchCopyOfBatch(*db, batch);
    EXPECT_EQ(copy.size(), batch.size());

    json::Value obj(json::ValueType::Object);
    db->getCountsJson(obj);

    ASSERT_TRUE(obj.isObject());
    for (json::StaticString const key :
         {jss::node_writes,
          jss::node_reads_total,
          jss::node_reads_hit,
          jss::node_written_bytes,
          jss::node_read_bytes,
          jss::node_reads_duration_us})
    {
        EXPECT_TRUE(obj.isMember(key)) << "missing key: " << static_cast<char const*>(key);
    }

    // One store per object in the batch.
    EXPECT_EQ(obj[jss::node_writes].asString(), std::to_string(batch.size()));
    // At least one read was served from the store.
    EXPECT_EQ(obj[jss::node_reads_hit].asString(), std::to_string(batch.size()));
}

// rq_bundle outside [1, 64] is rejected at construction.
TEST(NodeStoreDatabaseConfig, RequestBundleValidation)
{
    DummyScheduler scheduler;
    beast::TempDir const nodeDb;
    Section nodeParams;
    nodeParams.set("type", "memory");
    nodeParams.set("path", nodeDb.path());

    beast::Journal const journal(TestSink::instance());

    auto make = [&] {
        return Manager::instance().makeDatabase(megabytes(4), scheduler, 2, nodeParams, journal);
    };

    {
        SCOPED_TRACE("rq_bundle too small");
        nodeParams.set("rq_bundle", "0");
        try
        {
            auto db = make();
            FAIL() << "expected runtime_error for rq_bundle=0";
        }
        catch (std::runtime_error const& e)
        {
            EXPECT_STREQ(e.what(), "Invalid rq_bundle");
        }
    }

    {
        SCOPED_TRACE("rq_bundle too large");
        nodeParams.set("rq_bundle", "65");
        try
        {
            auto db = make();
            FAIL() << "expected runtime_error for rq_bundle=65";
        }
        catch (std::runtime_error const& e)
        {
            EXPECT_STREQ(e.what(), "Invalid rq_bundle");
        }
    }

    {
        SCOPED_TRACE("rq_bundle within range");
        nodeParams.set("rq_bundle", "8");
        EXPECT_NO_THROW({ auto db = make(); });
    }
}

}  // namespace xrpl::NodeStore
