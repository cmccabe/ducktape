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

#from ducktape.tests.stress import FaultInjector

# Check that the stress test annotation works.
from ducktape.mark._mark import stress_test, STRESS_TEST, Mark
from ducktape.tests.test import Test, TestContext


@stress_test
class MyStressTest(Test):
    def __init__(self, test_context):
        super(MyStressTest, self).__init__(test_context=test_context)

class CheckStressTest(object):
    """
    Check that MyStressTest is marked as a stress test.
    """
    def check_stress_test_annotation_applied(self):
        test = MyStressTest(TestContext())
        assert Mark.marked(test, STRESS_TEST)
