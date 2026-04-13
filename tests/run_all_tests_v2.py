from test3_bronze import run_bronze_tests
from test2_silver import run_silver_tests
from test_gold_v2 import run_gold_v2_tests


def run_all_tests_v2() -> None:
    print("\n==============================")
    print("RUNNING BRONZE TESTS")
    print("==============================")
    run_bronze_tests()

    print("\n==============================")
    print("RUNNING SILVER TESTS")
    print("==============================")
    run_silver_tests()

    print("\n==============================")
    print("RUNNING GOLD V2 TESTS")
    print("==============================")
    run_gold_v2_tests()

    print("\nAll V2 tests complete.")


if __name__ == "__main__":
    run_all_tests_v2()