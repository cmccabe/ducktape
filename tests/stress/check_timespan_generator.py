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
import random

import math

from ducktape.stress.timespan_generator import UniformTimespanGenerator, ConstantTimespanGenerator, \
    ExponentialTimespanGenerator


class CheckTimespanGenerator(object):
    def check_uniform_timespan_generator(self):
        """
        Check the uniform timespan generator.

        It should generate timespans in a given range.
        Given a specifc random seed, its output should be predictable.
        """
        gen = UniformTimespanGenerator(100, 1000)
        timespans = []
        rand = random.Random()
        rand.seed(123)
        for i in range(0, 1000):
            timespan = gen.generate(rand)
            assert(timespan >= 100.0)
            assert(timespan <= 1000.0)
            timespans.append(timespan)
        rand.seed(123)
        for i in range(0, 1000):
            timespan = gen.generate(rand)
            assert(timespan == timespans[i])

    def check_exponential_timespan_generator(self):
        """
        Check the exponential timespan generator.

        It should generate timestamps that are non-negative, with an
        average of 1 second.
        """
        rate = 1
        gen = ExponentialTimespanGenerator(rate)
        rand = random.Random()
        rand.seed(123)
        num_iterations = 100000
        sum = 0.0
        for i in range(0, num_iterations):
            timespan = gen.generate(rand)
            assert(timespan >= 0)
            sum = sum + timespan
        avg = sum / num_iterations
        assert(math.fabs(avg -  rate) < 0.25)

    def check_constant_timespan_generator(self):
        """
        Check the constant timespan generator.

        It should generate the same timespans over and over.
        """
        gen = ConstantTimespanGenerator([1, 2, 3])
        rand = random.Random()
        rand.seed(123)
        for i in range(0, 10):
            assert(1 == gen.generate(rand));
            assert(2 == gen.generate(rand));
            assert(3 == gen.generate(rand));
