include(FetchContent)

FetchContent_Declare(
  btrblocks
  GIT_REPOSITORY "https://github.com/PaulLampe/btrblocks.git"
  GIT_TAG arrow-2
  GIT_SHALLOW TRUE
)

FetchContent_MakeAvailable(btrblocks)

FetchContent_GetProperties(btrblocks)