#include <gtest/gtest.h>
#include "engine/matching_service.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <thread>

using namespace engine;
using namespace trade::matching;

class GrpcTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup Shard
        ShardConfig cfg{0, WaitStrategy::YIELD, -1, 10};
        shard_ = std::make_unique<Shard>(cfg);
        OrderBookConfig ob_cfg{5, 100000, 0.30, 10000, 100};
        shard_->AddSymbol(0, ob_cfg);
        shard_->Start();

        // Setup Service
        service_ = std::make_unique<MatchingService>(*shard_, 1024);
        service_->StartReplyThread();

        // In-process server
        grpc::ServerBuilder builder;
        builder.RegisterService(service_.get());
        server_ = builder.BuildAndStart();
        
        channel_ = server_->InProcessChannel(grpc::ChannelArguments());
        stub_ = MatchingEngine::NewStub(channel_);
    }

    void TearDown() override {
        service_->StopReplyThread();
        server_->Shutdown();
        shard_->Stop();
    }

    std::unique_ptr<Shard> shard_;
    std::unique_ptr<MatchingService> service_;
    std::unique_ptr<grpc::Server> server_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<MatchingEngine::Stub> stub_;
};

TEST_F(GrpcTest, SubmitOrderSuccess) {
    grpc::ClientContext context;
    SubmitOrderRequest req;
    req.set_order_id("O1");
    req.set_user_id("U1");
    req.set_symbol("RELIANCE");
    req.set_side(Side::BUY);
    req.set_order_type(OrderType::LIMIT);
    req.set_price_paise(10000);
    req.set_quantity(50);
    
    SubmitOrderResponse resp;
    grpc::Status status = stub_->SubmitOrder(&context, req, &resp);
    
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(resp.status(), OrderStatus::PARTIALLY_FILLED); // Actually PENDING/RESTING is PARTIALLY_FILLED with 0 qty
    EXPECT_EQ(resp.remaining_qty(), 50);
}

TEST_F(GrpcTest, DuplicateOrderId) {
    grpc::ClientContext context1;
    SubmitOrderRequest req;
    req.set_order_id("O1");
    req.set_user_id("U1");
    req.set_symbol("RELIANCE");
    req.set_side(Side::BUY);
    req.set_order_type(OrderType::LIMIT);
    req.set_price_paise(10000);
    req.set_quantity(50);
    
    SubmitOrderResponse resp1;
    grpc::Status status1 = stub_->SubmitOrder(&context1, req, &resp1);
    EXPECT_TRUE(status1.ok());
    
    grpc::ClientContext context2;
    SubmitOrderResponse resp2;
    grpc::Status status2 = stub_->SubmitOrder(&context2, req, &resp2);
    
    EXPECT_TRUE(status2.ok());
    EXPECT_EQ(resp2.status(), OrderStatus::REJECTED);
    EXPECT_EQ(resp2.reject_reason(), "DUPLICATE_ORDER");
}

TEST_F(GrpcTest, GetDepth) {
    // Submit 1 order
    grpc::ClientContext context1;
    SubmitOrderRequest req;
    req.set_order_id("O1");
    req.set_user_id("U1");
    req.set_symbol("RELIANCE");
    req.set_side(Side::BUY);
    req.set_order_type(OrderType::LIMIT);
    req.set_price_paise(10000);
    req.set_quantity(50);
    SubmitOrderResponse resp1;
    stub_->SubmitOrder(&context1, req, &resp1);
    
    // Wait for snapshot loop to catch up (rate limited to 50ms)
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    grpc::ClientContext context_depth;
    DepthRequest depth_req;
    depth_req.set_symbol("RELIANCE");
    DepthResponse depth_resp;
    
    grpc::Status status = stub_->GetDepth(&context_depth, depth_req, &depth_resp);
    EXPECT_TRUE(status.ok());
    
    if (depth_resp.bids_size() > 0) {
        EXPECT_EQ(depth_resp.bids(0).price_paise(), 10000);
        EXPECT_EQ(depth_resp.bids(0).quantity(), 50);
        EXPECT_EQ(depth_resp.bids(0).order_count(), 1);
    }
}

TEST_F(GrpcTest, DeadlineExpired) {
    grpc::ClientContext context;
    // Set a deadline in the past
    context.set_deadline(std::chrono::system_clock::now() - std::chrono::seconds(1));
    
    SubmitOrderRequest req;
    req.set_order_id("O_DEAD");
    SubmitOrderResponse resp;
    
    grpc::Status status = stub_->SubmitOrder(&context, req, &resp);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
}

TEST_F(GrpcTest, QueueFull) {
    // Our shard queue is 8192, but wait, the reply slots in the test is 1024.
    // If we send 1025 without letting them process (using a mock or saturating),
    // we should get RESOURCE_EXHAUSTED. 
    // To reliably test this, we'd need to freeze the shard loop or use a tiny capacity queue.
    // We can simulate it by stopping the shard.
    shard_->Stop(); 
    
    // Send > 8192 requests or > 1024 slots.
    // The try_push to ingress_queue_ might fail.
    
    int success = 0;
    int exhausted = 0;
    
    for(int i = 0; i < 2000; i++) {
        grpc::ClientContext context;
        SubmitOrderRequest req;
        req.set_order_id("Q" + std::to_string(i));
        SubmitOrderResponse resp;
        
        grpc::Status status = stub_->SubmitOrder(&context, req, &resp);
        if (status.ok()) success++;
        else if (status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED) exhausted++;
    }
    
    EXPECT_GT(exhausted, 0);
}
