#!/usr/bin/env python

"""
Simple EMA schedule generator. Outputs to stdout.

Usage:
  ema_gen.py [options] <record_id> <sched_field> <start_time> <days> <samples_per_day> <sampling_min> <gap_min>
  ema_gen.py interval_test <iters> <num_samples> <sampling_minutes> <gap_minutes>

Options:
  --rec-field=<str>         Name of the record_id field [default: record_id]
  --event-name=<str>        Name of the REDCap event, if it's longitudinal
  --start-date=<date>       Date to start generating; [default: tomorrow]
  --instrument-field=<str>  Name of the redcap_repeat_instrument field
  -v --verbose              Print debugging information

"""

# Note: This code works only when it runs in the same timezone as all
# participants. Our datetimes are timezone- and DST-naive, so as long
# as everyone is in US Central time, we're all set. If we have people
# in different time zones, we'll need to convert all the datetimes to
# local time.

import csv
from datetime import datetime, timedelta
import random
import sys

from docopt import docopt

import logging

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_ITERS = 1000


def random_minute_offsets(num_samples, total_minutes, gap_minutes, day_jitter_scale=0):
    """
    Return num_samples samples, over total_minutes time, separated by
    at least gap_minutes. Values are integer numbers of minutes.

    It divides the day into num_samples periods, randomly (from a uniform
    distribution) places samples in those periods, and rerolls if any samples
    are closer than gap_minutes together.

    TODO: See if we can fix the thing where the distribution of times isn't
          flat.
    """
    day_jitter_max = gap_minutes * day_jitter_scale
    total_minutes = total_minutes - day_jitter_max
    day_jitter = random.randrange(day_jitter_max + 1)
    fragment_length = total_minutes // num_samples
    iter = 0
    while iter < MAX_ITERS:
        time_offsets = [
            random.randrange(fragment_length) + (i * fragment_length) + day_jitter
            for i in range(num_samples)
        ]
        logger.debug(f"Iter {iter}: Generated {time_offsets}")
        if num_samples == 1:
            return time_offsets
        diffs = [
            (time_offsets[i + 1] - time_offsets[i])
            for i in range(len(time_offsets) - 1)
        ]
        if min(diffs) > gap_minutes:
            return time_offsets
        iter += 1
    return None


def random_timedeltas(num_samples, total_minutes, gap_minutes):
    return [
        timedelta(minutes=ofs)
        for ofs in random_minute_offsets(num_samples, total_minutes, gap_minutes)
    ]


# def make_sample_times(start_datetime, offsets):
#     """
#     See the daylight savings warning on time_str_to_delta
#     """
#     return [start_datetime + ofs for ofs in offsets]


def time_str_to_delta(time_string):
    """
    Takes a time formatted like "15:00 or "3:00 pm" or "3:00PM" and returns a
    timedelta that'll get you from midnight to there.
    """
    normed = time_string.upper().replace(" ", "")
    format = "%H:%M"
    if "M" in normed:
        format = "%I:%M%p"
    parsed = datetime.strptime(normed, format)
    delta = timedelta(hours=parsed.hour, minutes=parsed.minute)
    return delta


def make_sample_times(date, start_delta, samples, sampling_min, gap_min):
    sample_deltas = random_timedeltas(samples, sampling_min, gap_min)
    sample_datetimes = [
        date + start_delta + sample_delta for sample_delta in sample_deltas
    ]
    return sample_datetimes


def str_to_date(date_str):
    """
    date_str could be "tomorrow" or something like "2024-06-27"
    In either case, return a real datetime, starting at midnight, on that date
    """
    gen_date = None
    if date_str.lower() == "tomorrow":
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        gen_date = tomorrow
    else:
        gen_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    return datetime.combine(gen_date, datetime.min.time())


def generate_schedule(
    start_date, start_delta, days, samples_per_day, sampling_min, gap_min
):
    """
    Generates datetimes for every sample we're going to collect.
    Returns a list of datetimes.
    """
    schedule = []
    for day in range(days):
        day_delta = timedelta(days=day)
        gen_date = start_date + day_delta
        sample_times = make_sample_times(
            gen_date, start_delta, samples_per_day, sampling_min, gap_min
        )
        logger.debug(f"Generated {sample_times}")
        schedule += sample_times
    return schedule


def schedule_to_dicts(
    schedule, record_id, record_field, instrument_field, event_name, sched_field
):
    """
    Turns the schedule into a dict formatted for import into REDCap.
    """
    rows = []
    for instance_num, ema_time in enumerate(schedule):
        row = {}
        row[record_field] = record_id
        if event_name is not None:
            row["redcap_event_name"] = event_name
        if instrument_field is not None:
            row["redcap_repeat_instrument"] = instrument_field
        row["redcap_repeat_instance"] = instance_num + 1
        row[sched_field] = ema_time.strftime("%Y-%m-%d %H:%M:00")
        rows.append(row)
    return rows


def main(
    record_id,
    record_field,
    instrument_field,
    sched_field,
    event_name,
    start_date_str,
    start_time_str,
    days_str,
    samples_per_day,
    sampling_min,
    gap_min,
):
    """
    Generate some CSV data.
    It'll have the columns:
    <record_field>
    redcap_event_name (if event_name is not None)
    redcap_repeat_instance
    <sched_field>

    It'll have as many rows as the value of <days> * <samples_per_day>

    <record_field> will be <record_id>
    We'll compute the values for <sched_field>
    """
    logger.debug("We are in main")
    start_date = str_to_date(start_date_str)
    logger.debug(f"Start date is {start_date}")
    days = int(days_str)
    start_delta = time_str_to_delta(start_time_str)
    samples = int(samples_per_day)
    sampling_min = int(sampling_min)
    gap_min = int(gap_min)
    schedule = generate_schedule(
        start_date, start_delta, days, samples, sampling_min, gap_min
    )
    logger.debug(f"Generated schedule with {len(schedule)} items: {schedule}")
    redcap_rows = schedule_to_dicts(
        schedule, record_id, record_field, instrument_field, event_name, sched_field
    )
    logger.debug(redcap_rows)
    writer = csv.DictWriter(sys.stdout, fieldnames=redcap_rows[0].keys())
    writer.writeheader()
    for row in redcap_rows:
        writer.writerow(row)


def interval_test(args):
    iters = int(args["<iters>"])
    samples = int(args["<num_samples>"])
    sample_min = int(args["<sampling_minutes>"])
    sample_gap = int(args["<gap_minutes>"])
    for i in range(iters):
        results = random_minute_offsets(samples, sample_min, sample_gap)
        logger.info(f"Run {i}: Solution: {results}")
        if results is None:
            logger.critical("Could not find a solution!")
            sys.exit(1)


if __name__ == "__main__":
    args = docopt(__doc__)
    if args["--verbose"]:
        logger.setLevel(logging.DEBUG)
    logger.debug("Called with args:")
    logger.debug(args)
    if args["interval_test"]:
        interval_test(args)

    else:
        main(
            args["<record_id>"],
            args["--rec-field"],
            args["--instrument-field"],
            args["<sched_field>"],
            args["--event-name"],
            args["--start-date"],
            args["<start_time>"],
            args["<days>"],
            args["<samples_per_day>"],
            args["<sampling_min>"],
            args["<gap_min>"],
        )
