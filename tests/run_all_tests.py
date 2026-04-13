from test_bronze import run_bronze_tests
from test_silver import run_silver_tests
from test_gold import run_gold_tests


def run_all_tests() -> None:
    print("\n==============================")
    print("RUNNING BRONZE TESTS")
    print("==============================")
    run_bronze_tests()

    print("\n==============================")
    print("RUNNING SILVER TESTS")
    print("==============================")
    run_silver_tests()

    print("\n==============================")
    print("RUNNING GOLD TESTS")
    print("==============================")
    run_gold_tests()

    print("\nAll tests complete.")


if __name__ == "__main__":
    run_all_tests()