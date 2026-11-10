#!/usr/bin/env python3
"""Build and run the isolated PS-11475 contract tests after a server build.

Usage: python3 ps11475_run_contract_tests.py /mnt/nvme1/bin/PS-11475
The production implementation is compiled without test-specific edits. Logging is
captured by the fixture, which supplies the two MyRocks engine accessors.
A second fixture supplies a small plugin registry for coordinator tests.
The production clone sources are included without test-specific edits.
"""
from pathlib import Path
import shlex
import subprocess
import sys

build = Path(sys.argv[1]).resolve()
source = Path(__file__).resolve().parents[2]
output = build / 'ps11475-contract-tests'
output.mkdir(exist_ok=True)
target = 'storage/rocksdb/CMakeFiles/rocksdb.dir/rdb_clone.cc.o'
commands = subprocess.check_output(
    ['ninja', '-t', 'commands', target], cwd=build, text=True)
args = shlex.split(commands.splitlines()[-1])
args = args[:args.index('-MD')]
args = [arg for arg in args if not arg.startswith(
    '-I' + str(source) + '/storage/rocksdb/rocksdb/third-party/gtest')]
args += [
    '-I' + str(source / 'extra/googletest/googletest-1.17.0/googletest/include'),
    '-ffunction-sections', '-fdata-sections']
objects = []
for name in ('ps11475_clone-t', 'ps11475_hton-t'):
    obj = output / (name + '.o')
    subprocess.run(args + ['-o', str(obj), '-c',
                   str(source / 'unittest/gunit' / (name + '.cc'))],
                   cwd=build, check=True)
    objects.append(str(obj))
libraries = [
    'storage/rocksdb/librocksdb_sources.a',
    'archive_output_directory/libmysys.a',
    'archive_output_directory/libstrings.a',
    'archive_output_directory/libmytime.a',
    'archive_output_directory/libbacktrace.a',
    'archive_output_directory/libzlib.a',
    'archive_output_directory/libzstd.a',
    'archive_output_directory/liblz4_lib.a', 'libgtest.a']
subprocess.run([
    '/usr/bin/g++-14', '-fuse-ld=mold', '-o', str(output / 'ps11475_clone-t'),
    *objects, '-Wl,--gc-sections', '-Wl,--start-group', *libraries,
    '-Wl,--end-group', '-lssl', '-lcrypto', '-lnuma', '-ldl', '-lpthread', '-lrt'
], cwd=build, check=True)
raise SystemExit(subprocess.run([
    str(output / 'ps11475_clone-t'),
    '--gtest_output=xml:' + str(output / 'results.xml'), *sys.argv[2:]
], cwd=build).returncode)
