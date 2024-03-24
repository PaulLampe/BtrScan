include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

FetchContent_Declare(gflags
	GIT_REPOSITORY	https://github.com/gflags/gflags.git
	GIT_TAG			master
)

FetchContent_GetProperties(gflags)
FetchContent_MakeAvailable(gflags)