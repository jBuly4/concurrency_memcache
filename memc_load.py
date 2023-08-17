#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import glob
import gzip
import logging
import memcache
import os
import sys

from multiprocessing import Lock, Manager, Pool, Value
from optparse import OptionParser
from threading import Thread

import appsinstalled_pb2


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])
WORKERS = 4
THREADS = 8
BATCH = 40000


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def serialize(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()

    return key, packed


def insert_appsinstalled(memc_addr, values, dry_run=False):
    try:
        if dry_run:
            logging.debug("%s - %s" % (memc_addr, values))
        else:
            memc = memcache.Client([memc_addr])
            memc.set_multi(values)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def threadings(lines, device_memc, opts, errors, processed):
    devices = {}

    for line in lines:
        line = line.strip()

        if not line:
            continue

        apps = parse_appsinstalled(line)
        if not apps:
            # with errors.get_lock():  # if performance is low - use without locks? using manager is enough and
            # manager.Value doesn't have get_lock(). In case the lock is needed then use it explicitly
            errors.value += 1
            continue

        memc_addr = device_memc.get(apps.dev_type)
        if not memc_addr:
            logging.error("Unknown device type: %s" % apps.dev_type)
            continue

        key, packed = serialize(apps)

        if memc_addr in devices:
            devices[memc_addr].update({key: packed})
        else:
            devices[memc_addr] = {key: packed}

    for memc_addr, values in devices.items():
        inserted = insert_appsinstalled(memc_addr, values, opts.dry)
        if inserted:
            processed.value += 1
        else:
            errors.value += 1


def run_thread(tasks, args):
    task = Thread(
            target=threadings,
            args=args
    )
    tasks.append(task)
    task.start()


def join_thread(tasks):
    for task in tasks:
        task.join()


def print_error_state(errors, processed):
    err_rate = float(errors.value) / processed.value
    if err_rate < NORMAL_ERR_RATE:
        logging.info(f"Acceptable error rate {err_rate}")
    else:
        logging.error(f"High error rate {err_rate} > {NORMAL_ERR_RATE}. Failed Load")


def process_file(fn, device_memc, opts, errors, processed):
    logging.info('Processing %s' % fn)
    fd = gzip.open(fn, 'rt')
    batch = []
    tasks = []

    for line in fd:
        batch.append(line)
        if len(batch) == BATCH:
            run_thread(tasks, (batch, device_memc, opts, errors, processed))
            batch.clear()

    if len(batch) > 0:
        run_thread(tasks, (batch, device_memc, opts, errors, processed))

    join_thread(tasks)

    if not processed:
        fd.close()
        dot_rename(fn)
        return

    print_error_state(errors, processed)
    fd.close()
    dot_rename(fn)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    with Manager() as manager:
        lock = manager.Lock()
        errors = manager.Value('i', 0, lock=lock)
        processed = manager.Value('i', 0, lock=lock)
        args = [(fn, device_memc, options, errors, processed) for fn in glob.iglob(options.pattern)]
        with Pool(WORKERS) as pool:
            pool.starmap(process_file, args)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="./data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
