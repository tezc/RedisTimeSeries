import math
import time

import pytest
import redis
from RLTest import Env
from test_helper_classes import SAMPLE_SIZE, _get_ts_info, TSInfo
from includes import *


def test_ignore_invalid_params():
    e = Env()
    r = e.getClusterConnectionIfNeeded()

    def expect_error(*args, **kwargs):
        with pytest.raises(redis.ResponseError):
            r.execute_command(*args, **kwargs)

    try:
        # maxtimediff and maxvaldiff must be given together
        expect_error('ts.create', 'key1', 'ignore')
        expect_error('ts.create', 'key1', 'ignore', 'labels', 'label1')
        expect_error('ts.create', 'key1', 'ignore', '3')
        expect_error('ts.create', 'key1', 'ignore', '3', 'labels', 'label')

        # invalid maxtimediff
        expect_error('ts.create', 'key1', 'ignore', '3.2', '5')
        expect_error('ts.add', 'key1', 'ignore', '3.2', '5')
        expect_error('ts.create', 'key1', 'ignore', 'invalid', '5')
        expect_error('ts.add', 'key1', 'ignore', 'invalid', '5')
        expect_error('ts.create', 'key1', 'ignore', '-3', '5')
        expect_error('ts.add', 'key1', 'ignore', '-3', '5')

        # invalid maxvaldiff
        expect_error('ts.create', 'key1', 'ignore', '3', 'invalid')
        expect_error('ts.add', 'key1', 'ignore', '3', 'invalid')
        expect_error('ts.create', 'key1', 'ignore', '3', '-5')
        expect_error('ts.add', 'key1', 'ignore', '3', '-5')
    finally:
        e.stop()


def test_ignore_create():
    e = Env()
    r = e.getClusterConnectionIfNeeded()

    try:
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '5', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1006', '3')
        r.execute_command('TS.ADD', 'key1', '1007', '8')
        r.execute_command('TS.ADD', 'key1', '1008', '10')
        r.execute_command('TS.ADD', 'key1', '1009', '15.0001')

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        # Create with ts.add
        r.execute_command('TS.ADD', 'key2', '1000', '1', 'IGNORE', '5', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key2', '1001', '2')
        r.execute_command('TS.ADD', 'key2', '1006', '3')
        r.execute_command('TS.ADD', 'key2', '1007', '8')
        r.execute_command('TS.ADD', 'key2', '1008', '10')
        r.execute_command('TS.ADD', 'key2', '1009', '21001.0000002')

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'21001.0000002']]
        actual = r.execute_command('TS.range', 'key2', 0, '+')
        assert actual == expected

        assert r.execute_command('TS.ADD', 'key2', '1010', '21003') == 1009
        assert r.execute_command('TS.ADD', 'key2', '1020', '22000') == 1020
        assert r.execute_command('TS.ADD', 'key2', '1022', '21998') == 1020
        assert r.execute_command('TS.ADD', 'key2', '1023', '21994') == 1023

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'21001.0000002'], [1020, b'22000'], [1023, b'21994']]
        actual = r.execute_command('TS.range', 'key2', 0, '+')
        assert actual == expected
    finally:
        e.stop()


def test_ignore_duplicate_policy():
    e = Env()
    r = e.getClusterConnectionIfNeeded()

    try:
        # Filter should not work without DUPLICATE_POLICY LAST
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '5', '5')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1006', '3')
        r.execute_command('TS.ADD', 'key1', '1007', '8')
        r.execute_command('TS.ADD', 'key1', '1008', '10')
        r.execute_command('TS.ADD', 'key1', '1009', '15.0001')

        expected = [[1000, b'1'], [1001, b'2'], [1006, b'3'], [1007, b'8'],
                    [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        # ON_DUPLICATE LAST will override key config
        r.execute_command('TS.ADD', 'key1', '1010', '16.0', 'ON_DUPLICATE', 'LAST')
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected
    finally:
        e.stop()


def test_ignore_madd():
    e = Env()
    r = e.getClusterConnectionIfNeeded()

    try:
        r.execute_command('TS.CREATE', '{tag}key1', 'IGNORE', '5', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.CREATE', '{tag}key2')
        r.execute_command('TS.MADD', '{tag}key1', '1000', '1', '{tag}key2', '1000', '1')
        r.execute_command('TS.MADD', '{tag}key1', '1001', '2', '{tag}key2', '1001', '2')
        r.execute_command('TS.MADD', '{tag}key1', '1006', '3', '{tag}key2', '1006', '3')
        r.execute_command('TS.MADD', '{tag}key1', '1007', '8', '{tag}key2', '1007', '8')
        r.execute_command('TS.MADD', '{tag}key1', '1008', '10', '{tag}key2', '1008', '10')
        r.execute_command('TS.MADD', '{tag}key1', '1009', '15.0001', '{tag}key2', '1009', '15.0001')

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', '{tag}key1', 0, '+')
        assert actual == expected

        expected = [[1000, b'1'], [1001, b'2'], [1006, b'3'], [1007, b'8'],
                    [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', '{tag}key2', 0, '+')
        assert actual == expected

        assert r.execute_command('TS.MADD', '{tag}key1', '1010', '15.0001', '{tag}key2', '1010', '16.0') == [1009, 1010]
        assert r.execute_command('TS.ADD', '{tag}key1', '1012', '16') == 1009

    finally:
        e.stop()


def test_ignore_restart_restore():
    e = Env()
    r = e.getClusterConnectionIfNeeded()

    try:
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '3', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1004', '3')
        r.execute_command('TS.ADD', 'key1', '1005', '8')
        r.execute_command('TS.ADD', 'key1', '1006', '10')
        r.execute_command('TS.ADD', 'key1', '1007', '15.0001')

        expected = [[1000, b'1'], [1004, b'3'], [1006, b'10'], [1007, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        r.execute_command('TS.ADD', 'key1', '1010', '16.0')
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        dump = r.execute_command('dump', 'key1')
        assert r.execute_command('del', 'key1') == 1
        assert r.execute_command('restore', 'key1', 0, dump) == b'OK'

        r.execute_command('TS.ADD', 'key1', '1010', '16.0')
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        info = _get_ts_info(r, 'key1')
        assert info.ignore_max_time_diff == 3
        assert info.ignore_max_val_diff == b'5'

    finally:
        e.stop()


def test_ignore_alter():
    e = Env()
    r = e.getClusterConnectionIfNeeded()

    try:
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '3', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1004', '3')
        r.execute_command('TS.ADD', 'key1', '1005', '8')
        r.execute_command('TS.ADD', 'key1', '1006', '10')
        r.execute_command('TS.ADD', 'key1', '1007', '15.0001')
        r.execute_command('TS.ADD', 'key1', '1010', '19')

        expected = [[1000, b'1'], [1004, b'3'], [1006, b'10'], [1007, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        assert r.execute_command('TS.ALTER', 'key1', 'IGNORE', '2', '4') == b'OK'
        info = _get_ts_info(r, 'key1')
        assert info.ignore_max_time_diff == 2
        assert info.ignore_max_val_diff == b'4'
        r.execute_command('TS.ADD', 'key1', '1010', '19')
        expected = [[1000, b'1'], [1004, b'3'], [1006, b'10'], [1007, b'15.0001'], [1010, b'19']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected
    finally:
        e.stop()