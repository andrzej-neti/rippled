#include <xrpl/basics/BasicConfig.h>
#include <xrpl/basics/Blob.h>
#include <xrpl/basics/base_uint.h>
#include <xrpl/beast/utility/Journal.h>
#include <xrpl/nodestore/Backend.h>
#include <xrpl/nodestore/Database.h>
#include <xrpl/nodestore/DummyScheduler.h>
#include <xrpl/nodestore/NodeObject.h>
#include <xrpl/nodestore/Scheduler.h>
#include <xrpl/nodestore/Types.h>
#include <xrpl/nodestore/detail/DatabaseRotatingImp.h>

#include <gtest/gtest.h>
#include <helpers/TestSink.h>

#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace xrpl::NodeStore {

namespace {

// A controllable in-memory Backend. Unlike the StubBackend used by the
// DatabaseNodeImp tests, this one actually retains stored objects so that
// store-routing and the writable->archive fetch fallback can be observed,
// and it records the bookkeeping calls (setDeletePath/sync) that rotation
// relies on.
class MapBackend : public Backend
{
public:
    enum class FetchBehavior { Normal, Throw, Corrupt, Unknown };

    explicit MapBackend(std::string name) : name_(std::move(name))
    {
    }

    // Observable state.
    std::map<uint256, std::shared_ptr<NodeObject>> objects;
    FetchBehavior fetchBehavior{FetchBehavior::Normal};
    int writeLoad{0};
    bool deletePathCalled{false};
    bool syncCalled{false};
    int fetchCalls{0};

    std::string
    getName() override
    {
        return name_;
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
    fetch(uint256 const& hash, std::shared_ptr<NodeObject>* pObject) override
    {
        ++fetchCalls;
        switch (fetchBehavior)
        {
            case FetchBehavior::Throw:
                throw std::runtime_error("map backend fetch failure");
            case FetchBehavior::Corrupt:
                return Status::DataCorrupt;
            case FetchBehavior::Unknown:
                return static_cast<Status>(99);  // not a known Status value
            case FetchBehavior::Normal:
                break;
        }

        auto const it = objects.find(hash);
        if (it == objects.end())
            return Status::NotFound;
        *pObject = it->second;
        return Status::Ok;
    }
    void
    store(std::shared_ptr<NodeObject> const& object) override
    {
        objects[object->getHash()] = object;
    }
    void
    storeBatch(Batch const& batch) override
    {
        for (auto const& object : batch)
            objects[object->getHash()] = object;
    }
    void
    sync() override
    {
        syncCalled = true;
    }
    void
    forEach(std::function<void(std::shared_ptr<NodeObject>)> f) override
    {
        for (auto const& [hash, object] : objects)
            f(object);
    }
    int
    getWriteLoad() override
    {
        return writeLoad;
    }
    void
    setDeletePath() override
    {
        deletePathCalled = true;
    }
    [[nodiscard]] int
    fdRequired() const override
    {
        return 1;
    }

private:
    std::string name_;
};

// Bundles a DatabaseRotatingImp with raw pointers to its initial writable
// and archive backends so tests can both drive and inspect them. The pointers
// remain valid because the test holds shared ownership alongside the database.
struct RotatingFixture
{
    DummyScheduler scheduler;
    beast::Journal journal{TestSink::instance()};
    Section config;

    std::shared_ptr<MapBackend> writable;
    std::shared_ptr<MapBackend> archive;
    std::unique_ptr<DatabaseRotatingImp> db;

    RotatingFixture()
        : writable(std::make_shared<MapBackend>("writable"))
        , archive(std::make_shared<MapBackend>("archive"))
    {
        db =
            std::make_unique<DatabaseRotatingImp>(scheduler, 1, writable, archive, config, journal);
    }

    Database&
    base() const
    {
        return *db;
    }
};

// Build a single node object with a non-empty payload (storeStats asserts the
// data size is at least the object count, so empty blobs are not allowed).
std::shared_ptr<NodeObject>
makeObject(std::uint8_t seed)
{
    uint256 hash;
    hash.begin()[0] = seed;
    Blob data(4, seed);
    return NodeObject::createObject(NodeObjectType::Ledger, std::move(data), hash);
}

}  // namespace

// Trivial accessors delegate to the writable backend.
TEST(NodeStoreDatabaseRotating, accessors_delegate_to_writable)
{
    RotatingFixture const f;
    f.writable->writeLoad = 42;

    EXPECT_EQ(f.base().getName(), "writable");
    EXPECT_EQ(f.base().getWriteLoad(), 42);
    EXPECT_TRUE(f.base().isSameDB(1, 999));  // logically one database
}

// store() writes only to the writable backend, never the archive.
TEST(NodeStoreDatabaseRotating, store_routes_to_writable_only)
{
    RotatingFixture const f;
    auto const obj = makeObject(1);

    Blob data(obj->getData());
    f.base().store(obj->getType(), std::move(data), obj->getHash(), 0);

    EXPECT_EQ(f.writable->objects.count(obj->getHash()), 1u);
    EXPECT_TRUE(f.archive->objects.empty());
}

// sync() is forwarded to the writable backend only.
TEST(NodeStoreDatabaseRotating, sync_delegates_to_writable)
{
    RotatingFixture const f;
    f.base().sync();

    EXPECT_TRUE(f.writable->syncCalled);
    EXPECT_FALSE(f.archive->syncCalled);
}

// A hit in the writable backend is returned without consulting the archive.
TEST(NodeStoreDatabaseRotating, fetch_prefers_writable)
{
    RotatingFixture const f;
    auto const obj = makeObject(2);
    f.writable->store(obj);

    auto const result = f.base().fetchNodeObject(obj->getHash(), 0);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->getHash(), obj->getHash());
    EXPECT_EQ(f.archive->fetchCalls, 0);  // archive never touched
}

// A miss in writable falls back to the archive. With duplicate=false the
// object is NOT copied forward into the writable backend.
TEST(NodeStoreDatabaseRotating, fetch_falls_back_to_archive_without_duplicate)
{
    RotatingFixture const f;
    auto const obj = makeObject(3);
    f.archive->store(obj);

    auto const result =
        f.base().fetchNodeObject(obj->getHash(), 0, FetchType::Async, /*duplicate=*/false);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->getHash(), obj->getHash());
    EXPECT_EQ(f.writable->objects.count(obj->getHash()), 0u);  // not promoted
}

// A miss in writable falls back to the archive. With duplicate=true the object
// is copied forward into the writable backend.
TEST(NodeStoreDatabaseRotating, fetch_falls_back_to_archive_with_duplicate)
{
    RotatingFixture const f;
    auto const obj = makeObject(4);
    f.archive->store(obj);

    auto const result =
        f.base().fetchNodeObject(obj->getHash(), 0, FetchType::Async, /*duplicate=*/true);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->getHash(), obj->getHash());
    EXPECT_EQ(f.writable->objects.count(obj->getHash()), 1u);  // promoted
}

// A hash absent from both backends yields nullptr.
TEST(NodeStoreDatabaseRotating, fetch_missing_returns_null)
{
    RotatingFixture const f;
    EXPECT_EQ(f.base().fetchNodeObject(makeObject(5)->getHash(), 0), nullptr);
}

// An exception thrown by a backend fetch is propagated to the caller.
TEST(NodeStoreDatabaseRotating, fetch_exception_is_rethrown)
{
    RotatingFixture const f;
    f.writable->fetchBehavior = MapBackend::FetchBehavior::Throw;

    EXPECT_THROW(f.base().fetchNodeObject(makeObject(6)->getHash(), 0), std::exception);
}

// A DataCorrupt status from a backend is treated as a miss (logged, nullptr).
TEST(NodeStoreDatabaseRotating, fetch_corrupt_status_returns_null)
{
    RotatingFixture const f;
    f.writable->fetchBehavior = MapBackend::FetchBehavior::Corrupt;
    f.archive->fetchBehavior = MapBackend::FetchBehavior::Corrupt;

    EXPECT_EQ(f.base().fetchNodeObject(makeObject(13)->getHash(), 0), nullptr);
}

// An unrecognized status value from a backend is treated as a miss.
TEST(NodeStoreDatabaseRotating, fetch_unknown_status_returns_null)
{
    RotatingFixture const f;
    f.writable->fetchBehavior = MapBackend::FetchBehavior::Unknown;
    f.archive->fetchBehavior = MapBackend::FetchBehavior::Unknown;

    EXPECT_EQ(f.base().fetchNodeObject(makeObject(14)->getHash(), 0), nullptr);
}

// rotate() installs the new writable backend, demotes the old writable to
// archive, flags the old archive for deletion, and reports the post-rotation
// names to the callback.
TEST(NodeStoreDatabaseRotating, rotate_swaps_backends_and_marks_old_archive)
{
    RotatingFixture f;
    auto* const oldArchive = f.archive.get();

    // An object living in the original writable backend.
    auto const carried = makeObject(7);
    f.writable->store(carried);

    auto newBackend = std::make_unique<MapBackend>("fresh");

    std::string reportedWritable;
    std::string reportedArchive;
    f.db->rotate(std::move(newBackend), [&](std::string const& w, std::string const& a) {
        reportedWritable = w;
        reportedArchive = a;
    });

    // Callback sees the new writable name and the demoted writable as archive.
    EXPECT_EQ(reportedWritable, "fresh");
    EXPECT_EQ(reportedArchive, "writable");

    // The new writable backend is now the active one.
    EXPECT_EQ(f.base().getName(), "fresh");

    // The previous archive was marked for path deletion and dropped.
    EXPECT_TRUE(oldArchive->deletePathCalled);

    // The demoted writable now serves reads as the archive: an object that was
    // in the old writable is still fetchable via the fallback path.
    auto const result = f.base().fetchNodeObject(carried->getHash(), 0);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->getHash(), carried->getHash());

    // A new store lands in the fresh writable, not the demoted one.
    auto const stored = makeObject(8);
    Blob data(stored->getData());
    f.base().store(stored->getType(), std::move(data), stored->getHash(), 0);
    EXPECT_EQ(f.base().getName(), "fresh");
    EXPECT_EQ(f.writable->objects.count(stored->getHash()), 0u);  // old writable untouched
}

// importDatabase copies every object from the source database into the
// writable backend. This also drives the (private) forEach override on the
// source, which iterates both of its backends.
TEST(NodeStoreDatabaseRotating, import_database_writes_to_writable)
{
    RotatingFixture const dest;

    // Source database carrying a batch across its own writable + archive.
    auto srcWritable = std::make_shared<MapBackend>("src-writable");
    auto srcArchive = std::make_shared<MapBackend>("src-archive");
    auto const a = makeObject(11);
    auto const b = makeObject(12);
    srcWritable->store(a);
    srcArchive->store(b);

    DummyScheduler scheduler;
    Section const config;
    beast::Journal const journal(TestSink::instance());
    DatabaseRotatingImp src(scheduler, 1, srcWritable, srcArchive, config, journal);

    dest.base().importDatabase(src);

    EXPECT_EQ(dest.writable->objects.count(a->getHash()), 1u);
    EXPECT_EQ(dest.writable->objects.count(b->getHash()), 1u);
    EXPECT_TRUE(dest.archive->objects.empty());  // import targets writable only
}

}  // namespace xrpl::NodeStore
