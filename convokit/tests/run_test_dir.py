from unittest import TestLoader, TextTestRunner
import sys

if __name__ == "__main__":
    loader = TestLoader()
    tests = loader.discover(f'./convokit/tests/{sys.argv[1]}',
                            top_level_dir='.')
    testRunner = TextTestRunner()
    test_results = testRunner.run(tests)

    if test_results.wasSuccessful():
        exit(0)
    else:
        exit(1)