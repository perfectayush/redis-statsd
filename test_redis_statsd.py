import unittest
from redis_statsd import RedisStats, PROMETHEUS_EXPORTER_GUAGES_MAP, StatsD

class TestRedisStats(unittest.TestCase):
    def setUp(self):
        with open('./redis_info_master.3.2.txt', 'r') as f:
            info_buffer = f.read()

        self.redis_stats = RedisStats(info_buffer, "cluster:test")

        self.statsd = StatsD('localhost', 1)


    def test_verify_all_metrics_in_info(self):
        for metric_name in PROMETHEUS_EXPORTER_GUAGES_MAP:
            self.assertTrue(metric_name in self.redis_stats.info)

    def test_guage_metrics(self):
        guage_metrics = list(self.redis_stats.guage_metrics())
        self.assertEqual(len(guage_metrics), 49)

        guage_metric_names = (e[0] for e in guage_metrics)

        for metric_name in PROMETHEUS_EXPORTER_GUAGES_MAP.values():
            normalized_metric_name = self.redis_stats.add_metric_prefix(metric_name)
            self.assertTrue(normalized_metric_name in guage_metric_names)


    def test_cmdstat(self):
        cmdstat_metrics = list(self.redis_stats.cmdstat_metrics())
        self.assertEqual(len(cmdstat_metrics), 80)

        for metric, type, value, tags in cmdstat_metrics:
            self.assertTrue(metric.startswith("redis_command"))
            self.assertTrue('cmd' in tags)
            self.assertEqual('g', type)

    def test_redis_stats_output(self):
        self.redis_stats.send_metrics(self.statsd)

    def tearDown(self) -> None:
        self.statsd.close()


# Executing the tests in the above test case class
if __name__ == "__main__":
  unittest.main()
