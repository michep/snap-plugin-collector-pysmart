#!/usr/bin/env python

# http://www.apache.org/licenses/LICENSE-2.0.txt
#
# Copyright 2017 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re
import time
import sys

from pySMART import DeviceList

import snap_plugin.v1 as snap
from shutilwhich import which

LOG = logging.getLogger(__name__)


class Smartmon(snap.Collector):

    def __init__(self, *args):
        self.init = True
        self.devices = []
        super(Smartmon, self).__init__(*args)

    def update_catalog(self, config):
        metrics = []
        # adds namespace elements (static and dynamic) via namespace methods
        for i in ("threshold", "value", "whenfailed", "worst", "type",
                  "updated", "raw"):
            metric = snap.Metric(version=1,
                                 Description="SMARTMON list of dynamic devices"
                                 " and attributes")
            metric.namespace.add_static_element("intel")                              # /0
            metric.namespace.add_static_element("smartmon")                           # /1
            # dynamic elements which are captured from the smartmontool
            metric.namespace.add_dynamic_element("interface", "device interface")     # /2
            metric.namespace.add_dynamic_element("device", "device name")             # /3
            metric.namespace.add_dynamic_element("num", "attribute number")           # /4
            metric.namespace.add_dynamic_element("attribute", "attribute name")       # /5
            # values of the attributes to collect
            metric.namespace.add_static_element(i)                                    # /6
            metrics.append(metric)

        metric = snap.Metric(version=1,
                             Description="SMARTMON list of dynamic devices"
                             " and attributes")
        metric.namespace.add_static_element("intel")                              # /0
        metric.namespace.add_static_element("smartmon")                           # /1
        # dynamic elements which are captured from the smartmontool
        metric.namespace.add_dynamic_element("interface", "device interface")     # /2
        metric.namespace.add_dynamic_element("device", "device name")             # /3
        metric.namespace.add_static_element("health")                             # /4
        metrics.append(metric)

        return metrics

    def get_config_policy(self):
        return snap.ConfigPolicy([
            ("intel", "smartmon"),
            [
                (
                    "smartctl_path",
                    snap.StringRule(required=True, default="smartctl")
                ),
                (
                    "sudo",
                    snap.BoolRule(required=True)
                )
            ]
        ])

    def collect(self, metrics):

        self.devices = []
        smartctl_path = metrics[0].config['smartctl_path']
        sudo = bool(metrics[0].config['sudo'])

        for device in DeviceList(smartctl_path = smartctl_path, sudo = sudo).devices:
            if not device.supports_smart:
                LOG.warning("Skipping %s >> %s.  SMART is not enabled.", device.interface, device.path)
            else:
                self.devices.append(device)

        metrics_found = []
        metrics_return = []
        # set the time before the loop in case the time changes as the metric
        # values are being set
        ts_now = time.time()
        # loop through each device and each attribute on the device and store
        # the value to metric

        metricsMetadata = self.update_catalog(None)

        healthMetric = next(metric for metric in metricsMetadata if metric.namespace[4].value == 'health')

        for dev in self.devices:
            # dev.attributes is the list of S.M.A.R.T. attributes avaible on
            # each device, may change depending on the devide
            _metrics = snap.Metric(
                namespace=[i for i in healthMetric.namespace])
                #,unit=metric.unit)
            #_metrics.tags = [(k, v) for k, v in metric.tags.items()]
            # set the dynamic device interface
            _metrics.namespace[2].value = dev.interface
            # set the dynamic device name
            _metrics.namespace[3].value = dev.name
            _metrics.data = 0 if dev.assessment == 'PASS' else 1
            _metrics.timestamp = ts_now
            _metrics.tags["serialnum"] = dev.serial
            metrics_found.append(_metrics)
            for att in dev.attributes:
                if att is not None:
                    # sets the metricTeReturn to the metrics class which
                    # inherits the namespace, unit, and tags from the
                    # metric in metrics
                    for metType in ("threshold", "value", "whenfailed", "worst", "type",
                              "updated", "raw"):
                        longMetric = next(metric for metric in metricsMetadata if len(metric.namespace > 6) and metric.namespace[6].value == metType)
                        _metrics = snap.Metric(
                            namespace=[i for i in longMetric.namespace])
                            #,unit=metric.unit)
                        #_metrics.tags = [(k, v) for k, v in metric.tags.items()]
                        # set the dynamic device interface
                        _metrics.namespace[2].value = dev.interface
                        # set the dynamic device name
                        _metrics.namespace[3].value = dev.name
                        # set the dynamic attribute number
                        _metrics.namespace[4].value = att.num
                        # set the dynamic attribute name
                        _metrics.namespace[5].value = att.name
                        # store the value into the metric data
                        if _metrics.namespace[6].value == "threshold":
                            _metrics.data = att.thresh
                        if _metrics.namespace[6].value == "value":
                            _metrics.data = att.value
                        if _metrics.namespace[6].value == "whenfailed":
                            _metrics.data = att.when_failed
                        if _metrics.namespace[6].value == "worst":
                            _metrics.data = att.worst
                        if _metrics.namespace[6].value == "type":
                            _metrics.data = att.type
                        if _metrics.namespace[6].value == "updated":
                            _metrics.data = att.updated
                        if _metrics.namespace[6].value == "raw":
                            _metrics.data = att.raw
                        _metrics.timestamp = ts_now
                        _metrics.tags["serialnum"] = dev.serial
                        metrics_found.append(_metrics)

        for mt in metrics:
            matching = self.lookup_metric_by_namespace(mt, metrics_found)
            if len(matching):
                metrics_return.extend(matching)

        return metrics_return

    def namespace2str(self, ns, verb = False):
        st = ''
        for e in ns:
            if verb:
                st = (st + '/' + "[" + e.name + "]") if e.name else (st + '/' + e.value)
            else:
                st = st + '/' + e.value
        return st

    def lookup_metric_by_namespace(self, lookupmetric, metrics):
        ret = []
        lookupns = self.namespace2str(lookupmetric.namespace)
        lookupns = lookupns.replace('/', '\/').replace('*', '.*') + '$'
        nsre = re.compile(lookupns)
        for met in metrics:
            ns = self.namespace2str(met.namespace)
            match = nsre.search(ns)
            if match:
                resultTags = lookupmetric.tags.copy()
                metTags = met.tags.copy()
                resultTags.update(metTags)
                met.unit = lookupmetric.unit
                met.tags = resultTags
                ret.append(met)
        return ret
