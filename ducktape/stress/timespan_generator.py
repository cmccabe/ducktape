# Copyright 2017 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import math


class TimespanGenerator(object):
    """
    Base class for random timespan generators.
    """
    def generate(self, rand):
        """
        Generate a random timespan.

        :param rand:                A random number generator
        :return:                    A random timespan in seconds.
        """
        raise NotImplemented()


class UniformTimespanGenerator(TimespanGenerator):
    """
    A random timespan generator that return a timespan uniformly distributed
    between a minimum and a maximum length.
    """
    def __init__(self, min, max):
        """
        Initialize a uniform random timespan generator.

        :param min:                 The minimum timespan to return.
        :param max:                 The maximum timespan to return.
        """
        self.min = min
        self.max = max

    def generate(self, rand):
        return rand.uniform(self.min, self.max)


class ExponentialTimespanGenerator(TimespanGenerator):
    """
    A random timespan generator that return a timespan distributed according
    to an exponential distribution.  With this distribution, the probability
    that a timespan will end in the next x minutes is F(x) = 1 - exp(-rate*x)
    This is the cumulative distribution function (CDF) of the exponential
    distribution.

    To get the next time delta, we take the inverse of this function.
    Basically, we are choosing a probability and solving for a time.
    To avoid waiting too long, we choose probabilities between 0% and 90%
    and leave out the higher percentages.
    """
    def __init__(self, rate):
        """
        Initialize an inverse exponential random timespan generator.

        :param rate:                The average number of times per second to trigger.
        """
        self.rate = rate

    def generate(self, rand):
        p = rand.uniform(0.0, 0.9)
        return -(math.log(p) / self.rate)


class ConstantTimespanGenerator(TimespanGenerator):
    """
    Generates a predetermined repeating set of timespans.
    """
    def __init__(self, spans):
        """
        Initialize an inverse exponential random timespan generator.

        :param spans:               An array of timespans to generate.
        """
        self.spans = spans
        self.cur = 0

    def generate(self, rand):
        next = self.spans[self.cur]
        self.cur = self.cur + 1
        if self.cur >= len(self.spans):
            self.cur = 0
        return next
