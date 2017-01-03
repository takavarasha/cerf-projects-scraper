#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Push the data to HDX """

from hdx.data.dataset import Dataset
import datetime


def _set_dataset_date(config):
    d = Dataset.read_from_hdx(config,'29d1cc01-cc72-4249-ab15-a01b6d10bfe9')
    d.set_dataset_date_from_datetime(datetime.datetime.now())
    d.update_in_hdx(update_gallery=False, update_resources=False)


def push(config):
    _set_dataset_date(config)
    return config
