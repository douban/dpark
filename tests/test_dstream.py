import os, sys, logging
import unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpark.dstream import *

logging.getLogger('dpark').setLevel(logging.ERROR)

class TestInputStream(InputDStream):
    def __init__(self, ssc, input, numPart=2):
        InputDStream.__init__(self, ssc)
        self.input = input
        self.numPart = numPart
        self.index = -1

    def compute(self, t):
        self.index += 1
        index = (t - self.zeroTime) / self.slideDuration - 1
        if 0 <= index < len(self.input):
            d = self.input[index]
            return self.ssc.sc.makeRDD(d, self.numPart)

class TestOutputStream(ForEachDStream):
    def __init__(self, parent, output):
        def collect(rdd, t):
            r = rdd.collect()
            #print 'collect', t, r
            return self.output.append(r)
        ForEachDStream.__init__(self, parent, collect)
        self.output = output


class TestDStream(unittest.TestCase):
    def _setupStreams(self, intput1, input2, operation):
        ssc = StreamingContext(2, "local")
        is1 = TestInputStream(ssc, intput1)
        ssc.registerInputStream(is1)
        if input2:
            is2 = TestInputStream(ssc, input2)
            ssc.registerInputStream(is2)
            os = operation(is1, is2)
        else:
            os = operation(is1)
        output = TestOutputStream(os, [])
        ssc.registerOutputStream(output)
        return ssc

    def _runStreams(self, ssc, numBatches, numExpectedOuput):
        output = ssc.graph.outputStreams[0].output
        try:
            #print 'expected', numExpectedOuput
            first = int(time.time()) - numBatches * ssc.batchDuration
            #print 'start', first, numBatches
            ssc.start(first)
            while len(output) < numExpectedOuput:
                time.sleep(.01)
        finally:
            ssc.stop()
        return output

    def _verifyOutput(self, output, expected, useSet):
        #self.assertEqual(len(output), len(expected))
        assert len(output) >= len(expected)
        #print output
        for i in range(len(expected)):
            #print i
            if useSet:
                self.assertEqual(set(output[i]), set(expected[i]))
            elif isinstance(output[i], list) and isinstance(expected[i], list):
                self.assertEqual(sorted(output[i]), sorted(expected[i]))
            else:
                self.assertEqual(output[i], expected[i])

    def _testOperation(self, input1, input2, operation, expectedOutput, numBatches=0, useSet=False):
        if numBatches <= 0:
            numBatches = len(expectedOutput)
        ssc = self._setupStreams(input1, input2, operation)
        output = self._runStreams(ssc, numBatches, len(expectedOutput))
        self._verifyOutput(output, expectedOutput, useSet)

class TestBasic(TestDStream):
    def test_map(self):
        d = [range(i*4, i*4+4) for i in range(4)]
        r = [[str(i) for i in row] for row in d]
        self._testOperation(d, None, lambda x: x.map(str), r, 4, False)
        r = [sum([range(x,x*2) for x in row], []) for row in d]
        self._testOperation(d, None, lambda x: x.flatMap(lambda x:range(x, x*2)), r)

    def test_filter(self):
        d = [range(i*4, i*4+4) for i in range(4)]
        self._testOperation(d, None, lambda x: x.filter(lambda y: y%2==0),
            [[i for i in row if i%2 ==0] for row in d])

    def test_glom(self):
        d = [range(i*4, i*4+4) for i in range(4)]
        r = [[row[:2], row[2:]] for row in d]
        self._testOperation(d, None, lambda s: s.glom().map(lambda x:list(x)), r)

    def test_mapPartitions(self):
        d = [range(i*4, i*4+4) for i in range(4)]
        r = [[sum(row[:2]), sum(row[2:])] for row in d]
        self._testOperation(d, None, lambda s: s.mapPartitions(lambda l: [reduce(lambda x,y:x+y, l)]), r)

    def test_groupByKey(self):
        d = [["a", "a", "b"], ["", ""], []]
        r = [[("a", [1, 1]), ("b", [1])], [("", [1,1])], []]
        self._testOperation(d, None, lambda s: s.map(lambda x:(x,1)).groupByKey(), r, useSet=False)

    def test_reduceByKey(self):
        d = [["a", "a", "b"], ["", ""], []]
        r = [[("a", 2), ("b", 1)], [("", 2)], []]
        self._testOperation(d, None, lambda s: s.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y), r, useSet=True)

    def test_reduce(self):
        d = [range(i*4, i*4+4) for i in range(4)]
        r = [[sum(row)] for row in d]
        self._testOperation(d, None, lambda s: s.reduce(lambda x,y:x+y), r)

    def test_cogroup(self):
        d1 = [["a", "a", "b"], ["a", ""], [""]]
        d2 = [["a", "a", "b"], ["b", ""], []]
        r = [[("a", ([1,1], ["x", "x"])), ("b", ([1,], ["x"]))],
             [("a", ([1], [])), ("b", ([], ["x"])), ("", ([1],["x"]))],
             [("", ([1],[]))],
        ]
        def op(s1, s2):
            return s1.map(lambda x:(x,1)).cogroup(s2.map(lambda x:(x,"x")))
        self._testOperation(d1, d2, op, r)

    def test_updateStateByKey(self):
        d = [["a"], ["a", "b",], ['a', 'b','c'], ['a','b'], ['a'], []]
        r = [[("a", 1)],
             [("a", 2), ("b", 1)],
             [("a", 3), ("b", 2), ("c", 1)],
             [("a", 4), ("b", 3), ("c", 1)],
             [("a", 5), ("b", 3), ("c", 1)],
             [("a", 5), ("b", 3), ("c", 1)],
        ]

        def op(s):
            def updatef(vs, state):
                return sum(vs) + (state or 0)
            return s.map(lambda x: (x,1)).updateStateByKey(updatef)
        self._testOperation(d, None, op, r, useSet=True)

    #def test_window(self):
    #    d = [range(i, i+1) for i in range(10)]
    #    def op(s):
    #        return s.map(lambda x:(x % 10, 1)).window(2, 1).window(4, 2)
    #    ssc = self._setupStreams(d, None, op)
    #    ssc.remember(3)
    #    self._runStreams(ssc, 10, 10/2)


class TestWindow(TestDStream):
    largerSlideInput = [
        [("a", 1)],
        [("a", 2)],
        [("a", 3)],
        [("a", 4)],
        [("a", 5)],
        [("a", 6)],
        [],
        [],
    ]
    largerSlideReduceOutput = [
        [("a", 3)],
        [("a", 10)],
        [("a", 18)],
        [("a", 11)],
    ]
    bigInput = [
        [("a", 1)],
        [("a", 1), ("b", 1)],
        [("a", 1), ("b", 1), ("c", 1)],
        [("a", 1), ("b", 1)],
        [("a", 1)],
        [],
        [("a", 1)],
        [("a", 1), ("b", 1)],
        [("a", 1), ("b", 1), ("c", 1)],
        [("a", 1), ("b", 1)],
        [("a", 1)],
        [],
    ]
    bigGroupByOutput = [
        [("a", [1])],
        [("a", [1, 1]), ("b", [1])],
        [("a", [1, 1]), ("b", [1, 1]), ("c", [1])],
        [("a", [1, 1]), ("b", [1, 1]), ("c", [1])],
        [("a", [1, 1]), ("b", [1])],
        [("a", [1])],
        [("a", [1])],
        [("a", [1, 1]), ("b", [1])],
        [("a", [1, 1]), ("b", [1, 1]), ("c", [1])],
        [("a", [1, 1]), ("b", [1, 1]), ("c", [1])],
        [("a", [1, 1]), ("b", [1])],
        [("a", [1])],
    ]
    bigReduceOutput = [
        [("a", 1)],
        [("a", 2), ("b", 1)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 1)],
        [("a", 1)],
        [("a", 1)],
        [("a", 2), ("b", 1)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 1)],
        [("a", 1)],
    ]
    bigReduceInvOutput = [
        [("a", 1)],
        [("a", 2), ("b", 1)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 1), ("c", 0)],
        [("a", 1), ("b", 0), ("c", 0)],
        [("a", 1), ("b", 0), ("c", 0)],
        [("a", 2), ("b", 1), ("c", 0)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 2), ("c", 1)],
        [("a", 2), ("b", 1), ("c", 0)],
        [("a", 1), ("b", 0), ("c", 0)],
    ]
    def _testWindow(self, input, expectedOutput, window=4, slide=2):
        self._testOperation(input, None, lambda s: s.window(window, slide), expectedOutput,
            len(expectedOutput) * slide / 2, useSet=True)

    def _testReduceByKeyAndWindow(self, input, expectedOutput, window=4, slide=2):
        self._testOperation(input, None, lambda s: s.reduceByKeyAndWindow(lambda x,y:x+y, None, window, slide),
            expectedOutput, len(expectedOutput) * slide / 2, useSet=True)

    def _testReduceByKeyAndWindowInv(self, input, expectedOutput, window=4, slide=2):
        self._testOperation(input, None,
            lambda s: s.reduceByKeyAndWindow(lambda x,y: x+y, lambda x,y: x-y, window, slide),
            expectedOutput, len(expectedOutput) * slide / 2, useSet=True)

    def test_window(self):
        # basic window
        self._testWindow([[i] for i in range(6)],
            [range(max(i-1, 0), i+1) for i in range(6)])
        # tumbling window
        self._testWindow([[i] for i in range(6)],
            [range(i*2, i*2+2) for i in range(3)], 4, 4)
        # large window
        self._testWindow([[i] for i in range(6)],
            [[0, 1], range(4), range(2, 6), range(4, 6)], 8, 4)
        # non-overlapping window
        self._testWindow([[i] for i in range(6)],
            [range(1, 3), range(4, 6)], 4, 6)

    def test_reduceByKeyAndWindow(self):
        # basic reduction
        self._testReduceByKeyAndWindow(
            [[("a", 1), ("a", 3)]],
            [[("a", 4)]]
        )
        # key already in window and new value added into window
        self._testReduceByKeyAndWindow(
            [[("a", 1)], [("a", 1)]],
            [[("a", 1)], [("a", 2)]],
        )
        # new key added to window
        self._testReduceByKeyAndWindow(
            [[("a", 1)], [("a", 1), ("b", 1)]],
            [[("a", 1)], [("a", 2), ("b", 1)]],
        )
        # new removed from window
        self._testReduceByKeyAndWindow(
            [[("a", 1)], [("a", 1)], [], []],
            [[("a", 1)], [("a", 2)], [("a", 1)], []],
        )
        # larger slide time
        self._testReduceByKeyAndWindow(
            self.largerSlideInput, self.largerSlideReduceOutput, 8, 4)
        # big test
        self._testReduceByKeyAndWindow(self.bigInput, self.bigReduceOutput)

    def test_reduce_and_window_inv(self):
        # basic reduction
        self._testReduceByKeyAndWindowInv(
            [[("a", 1), ("a", 3)]],
            [[("a", 4)]]
        )
        # key already in window and new value added into window
        self._testReduceByKeyAndWindowInv(
            [[("a", 1)], [("a", 1)]],
            [[("a", 1)], [("a", 2)]],
        )
        # new key added to window
        self._testReduceByKeyAndWindowInv(
            [[("a", 1)], [("a", 1), ("b", 1)]],
            [[("a", 1)], [("a", 2), ("b", 1)]],
        )
        # new removed from window
        self._testReduceByKeyAndWindowInv(
            [[], []],
            [[], []],
        )
        self._testReduceByKeyAndWindowInv(
            [[("a", 1)], [("a", 1)], [], []],
            [[("a", 1)], [("a", 2)], [("a", 1)], [("a", 0)]],
        )
        # large slide time
        self._testReduceByKeyAndWindowInv(self.largerSlideInput,
            self.largerSlideReduceOutput, 8, 4)
        # big test
        self._testReduceByKeyAndWindowInv(self.bigInput, self.bigReduceInvOutput)

    def test_group_by_window(self):
        r = [[(k,set(v))  for k,v in row] for row in self.bigGroupByOutput]
        def op(s):
            return s.groupByKeyAndWindow(4, 2).mapValues(lambda x:set(x))
        self._testOperation(self.bigInput, None, op, r)

    def test_count_by_window(self):
        d = [[1], [1], [1,2], [0], [], []]
        r =  [[1], [2], [3], [3], [1], [0]]
        self._testOperation(d, None, lambda s: s.countByWindow(4, 2), r)

    def test_count_by_key_and_window(self):
        d = [[("a", 1)], [("b", 1), ("b", 2)], [("a", 10), ("b", 20)]]
        r = [[("a", 1)], [("a", 1), ("b", 2)], [("a", 1), ("b", 3)]]
        self._testOperation(d, None, lambda s: s.countByKeyAndWindow(4, 2), r)


class TestInputDStream(TestDStream):
    def test_input_stream(self):
        pass

    def test_failure(self):
        pass

    def test_checkpoint(self):
        pass


if __name__ == '__main__':
    import logging
    unittest.main()
