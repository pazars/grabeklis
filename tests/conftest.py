import pytest


def pytest_addoption(parser):
    parser.addoption("--spider", action="store", help="Spider directory to test")
    parser.addoption("--dir", action="store", help="Single run directory to test")


def pytest_configure(config):
    global spider_dir, run_dir
    spider_dir = config.getoption("spider")
    run_dir = config.getoption("dir")
