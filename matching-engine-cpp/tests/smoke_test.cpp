#include <gtest/gtest.h>

#include "engine/version.h"

TEST(SmokeTest, VersionIsZeroDotOne) {
    EXPECT_EQ(engine::kVersionMajor, 0);
    EXPECT_EQ(engine::kVersionMinor, 1);
    EXPECT_EQ(engine::kVersionPatch, 0);
}
