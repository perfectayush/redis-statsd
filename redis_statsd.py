#!/usr/bin/env python

import argparse
import socket


def parse_args():
    parser = argparse.ArgumentParser(description='Collect metrics from Redis and emit to StatsD')
    parser.add_argument('--prefix', dest='prefix', type=str, default='redis', help='The prefix to use for metric names')
    parser.add_argument('--redis-host', dest='redis_host', type=str, default='localhost:6379', help='The address and port of the Redis host to connect to')
    parser.add_argument('--statsd-host', dest='statsd_host', type=str, default='localhost:8125', help='The address and port of the StatsD host to connect to')
    parser.add_argument('--global-tags', dest='global_tags', type=str, help='Global tags to add to all metrics')

    args = parser.parse_args()
    return args


# A mapping of metrics name for compatibility with redis_exporter
# Taken from https://github.com/oliver006/redis_exporter/blob/master/exporter/exporter.go
# Glossary for some commented metrics
# #NA: Not available in redis 3.2
# #ST: String Value type
# #DI: Disabled metric type

PROMETHEUS_EXPORTER_GUAGES_MAP = {
    # Server
    "uptime_in_seconds": "uptime_in_seconds",
	"process_id":        "process_id",


    # Clients
	"connected_clients": "connected_clients",
	"blocked_clients":   "blocked_clients",
	#NA "tracking_clients":  "tracking_clients",

	# redis 2,3,4.x
	"client_longest_output_list": "client_longest_output_list",
	"client_biggest_input_buf":   "client_biggest_input_buf",

	# the above two metrics were renamed in redis 5.x
	#NA "client_recent_max_output_buffer": "client_recent_max_output_buffer_bytes",
	#NA "client_recent_max_input_buffer":  "client_recent_max_input_buffer_bytes",

	# Memory
	#NA "allocator_active":     "allocator_active_bytes",
	#NA "allocator_allocated":  "allocator_allocated_bytes",
	#NA "allocator_resident":   "allocator_resident_bytes",
	#NA "allocator_frag_ratio": "allocator_frag_ratio",
	#NA "allocator_frag_bytes": "allocator_frag_bytes",
	#NA "allocator_rss_ratio":  "allocator_rss_ratio",
	#NA "allocator_rss_bytes":  "allocator_rss_bytes",

	"used_memory":          "memory_used_bytes",
	"used_memory_rss":      "memory_used_rss_bytes",
	"used_memory_peak":     "memory_used_peak_bytes",
	"used_memory_lua":      "memory_used_lua_bytes",
	#NA "used_memory_overhead": "memory_used_overhead_bytes",
	#NA "used_memory_startup":  "memory_used_startup_bytes",
	#NA "used_memory_dataset":  "memory_used_dataset_bytes",
	#NA "used_memory_scripts":  "memory_used_scripts_bytes",
	"maxmemory":            "memory_max_bytes",

	#NA "maxmemory_reservation":         "memory_max_reservation_bytes",
	#NA "maxmemory_desired_reservation": "memory_max_reservation_desired_bytes",

	#NA "maxfragmentationmemory_reservation":         "memory_max_fragmentation_reservation_bytes",
	#NA "maxfragmentationmemory_desired_reservation": "memory_max_fragmentation_reservation_desired_bytes",

	"mem_fragmentation_ratio": "mem_fragmentation_ratio",
	#NA "mem_fragmentation_bytes": "mem_fragmentation_bytes",
	#NA "mem_clients_slaves":      "mem_clients_slaves",
	#NA "mem_clients_normal":      "mem_clients_normal",

	#  https://github.com/antirez/redis/blob/17bf0b25c1171486e3a1b089f3181fff2bc0d4f0/src/evict.c#L349-L352
	#  ... the sum of AOF and slaves buffer ....
	#NA "mem_not_counted_for_evict": "mem_not_counted_for_eviction_bytes",

	#NA "lazyfree_pending_objects": "lazyfree_pending_objects",
	#NA "active_defrag_running":    "active_defrag_running",

	"migrate_cached_sockets": "migrate_cached_sockets_total",

	#NA "active_defrag_hits":       "defrag_hits",
	#NA "active_defrag_misses":     "defrag_misses",
	#NA "active_defrag_key_hits":   "defrag_key_hits",
	#NA "active_defrag_key_misses": "defrag_key_misses",

	#  https://github.com/antirez/redis/blob/0af467d18f9d12b137af3b709c0af579c29d8414/src/expire.c#L297-L299
	#NA "expired_time_cap_reached_count": "expired_time_cap_reached_total",

	# Persistence
	"loading":                      "loading_dump_file",
	"rdb_changes_since_last_save":  "rdb_changes_since_last_save",
	"rdb_bgsave_in_progress":       "rdb_bgsave_in_progress",
	"rdb_last_save_time":           "rdb_last_save_timestamp_seconds",
	#ST "rdb_last_bgsave_status":       "rdb_last_bgsave_status",
	"rdb_last_bgsave_time_sec":     "rdb_last_bgsave_duration_sec",
	"rdb_current_bgsave_time_sec":  "rdb_current_bgsave_duration_sec",
	#NA "rdb_last_cow_size":            "rdb_last_cow_size_bytes",
	"aof_enabled":                  "aof_enabled",
	"aof_rewrite_in_progress":      "aof_rewrite_in_progress",
	"aof_rewrite_scheduled":        "aof_rewrite_scheduled",
	"aof_last_rewrite_time_sec":    "aof_last_rewrite_duration_sec",
	"aof_current_rewrite_time_sec": "aof_current_rewrite_duration_sec",
	#NA "aof_last_cow_size":            "aof_last_cow_size_bytes",
	#NA "aof_current_size":             "aof_current_size_bytes",
	#NA "aof_base_size":                "aof_base_size_bytes",
	#NA "aof_pending_rewrite":          "aof_pending_rewrite",
	#NA "aof_buffer_length":            "aof_buffer_length",
	#NA "aof_rewrite_buffer_length":    "aof_rewrite_buffer_length",
	#NA "aof_pending_bio_fsync":        "aof_pending_bio_fsync",
	#NA "aof_delayed_fsync":            "aof_delayed_fsync",
	#ST "aof_last_bgrewrite_status":    "aof_last_bgrewrite_status",
	#ST "aof_last_write_status":        "aof_last_write_status",
	#NA "module_fork_in_progress":      "module_fork_in_progress",
	#NA "module_fork_last_cow_size":    "module_fork_last_cow_size",

	# Stats
	"pubsub_channels":  "pubsub_channels",
	"pubsub_patterns":  "pubsub_patterns",
	"latest_fork_usec": "latest_fork_usec",

	# Replication
	"connected_slaves":               "connected_slaves",
	"repl_backlog_size":              "replication_backlog_bytes",
	"repl_backlog_active":            "repl_backlog_is_active",
	"repl_backlog_first_byte_offset": "repl_backlog_first_byte_offset",
	"repl_backlog_histlen":           "repl_backlog_history_bytes",
	"master_repl_offset":             "master_repl_offset",
	#NA "second_repl_offset":             "second_repl_offset",
	#NA "slave_expires_tracked_keys":     "slave_expires_tracked_keys",
	#NA "slave_priority":                 "slave_priority",
	"sync_full":                      "replica_resyncs_full",
	"sync_partial_ok":                "replica_partial_resync_accepted",
	"sync_partial_err":               "replica_partial_resync_denied",

	# Cluster
	#NA "cluster_stats_messages_sent":     "cluster_messages_sent_total",
	#NA "cluster_stats_messages_received": "cluster_messages_received_total",

	## addtl. KeyDB metrics
	#NA "server_threads":        "server_threads_total",
	#NA "long_lock_waits":       "long_lock_waits_total",
	#NA "current_client_thread": "current_client_thread",


    ## Counters from redis_exporter
	"total_connections_received": "connections_received_total",
	"total_commands_processed":   "commands_processed_total",

	"rejected_connections":   "rejected_connections_total",
	"total_net_input_bytes":  "net_input_bytes_total",
	"total_net_output_bytes": "net_output_bytes_total",

	"expired_keys":    "expired_keys_total",
	"evicted_keys":    "evicted_keys_total",
	"keyspace_hits":   "keyspace_hits_total",
	"keyspace_misses": "keyspace_misses_total",

	"used_cpu_sys":           "cpu_sys_seconds_total",
	"used_cpu_user":          "cpu_user_seconds_total",
	"used_cpu_sys_children":  "cpu_sys_children_seconds_total",
	"used_cpu_user_children": "cpu_user_children_seconds_total",
}

class StatsD:
    """
    Class to flush metrics to statsd

    Supports DogStatsD style tags for metrics
    """
    def __init__(self, statsd_host, statsd_port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.host = statsd_host
        self.port = int(statsd_port)

    def send(self, string):
        print(string)
        self.sock.sendto(string.encode('utf-8'), (self.host, self.port))

    @staticmethod
    def make_dogstatd_tagstring(tags):
        """
        Make dogstatsd style tagstring for adding labels to statsd_exporter
        """
        if tags:
            tagstring = ",".join(
                [f"{key}:{value}" for key,value in tags.items()]
            )
            return "|#{}".format(tagstring)
        else:
            return ""

    def send_metric(self, name, type, value, tags=None):
        assert type in ['c', 'g']

        tagstring = StatsD.make_dogstatd_tagstring(tags)

        statsd_metric = '{}:{}|{}{}'.format(name, value, type, tagstring)
        self.send(statsd_metric)

    def close(self):
        self.sock.close()


def redis_get_info(redis_host, redis_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((redis_host, int(redis_port)))
    s.send("INFO ALL\n".encode('utf-8'))

    info_buffer = s.recv(4096).decode('utf-8')
    while not info_buffer.endswith("\r\n"):
        info_buffer += s.recv(4096).decode('utf-8')

    s.close()

    return info_buffer

class RedisStats:
    """
    Class for parsing and processing stats in info buffer
    """
    def __init__(self, info_buffer, global_tags='', metric_prefix='redis'):
        "Parse redis info buffer to a dictionary"
        self.info = RedisStats.parse_info_buffer(info_buffer)
        self.global_tags = RedisStats.parse_global_tags(global_tags)
        self.add_metric_prefix = lambda metric: f"{metric_prefix}_{metric}"

    @staticmethod
    def parse_info_buffer(info_buffer):
        info_line_splitter = (
            line.split(':', 1)
            for line in info_buffer.splitlines()
            if ":" in line
        )
        return { key: value for key, value in info_line_splitter }

    @staticmethod
    def parse_global_tags(global_tags):
        if global_tags:
            tags = {
                key: value
                for key, value in
                (tags.split(':', 1) for tags in global_tags.split(','))
            }
        else:
            tags = {}

        return tags

    def guage_metrics(self):
        guage_metrics = {
            final_metric_key: self.info[info_dict_key]
            for info_dict_key, final_metric_key in PROMETHEUS_EXPORTER_GUAGES_MAP.items()
            if info_dict_key in self.info
        }

        for metric, value in guage_metrics.items():
            yield (self.add_metric_prefix(metric), 'g', value, self.global_tags)

    def cmdstat_metrics(self):
        """
        Process redis metrics starting with cmdstat
        Example line from info_dict:
        {"cmdstat_get":"calls=13102161354,usec=23597902067,usec_per_call=1.80"}
        """

        def processs_cmdstat_metrics(metrics):
            metric_generator = (metric.split("=") for metric in metrics.split(',') )
            metrics = {
                key: int(value)
                for key, value in metric_generator
                if key in ['calls', 'usec']
            }
            return metrics

        get_cmdstat_type = lambda metric_key: metric_key.split("_", 1)[1]

        cmdstat_dict = {
            get_cmdstat_type(key): processs_cmdstat_metrics(value)
            for key, value in self.info.items()
            if key.startswith("cmdstat")
        }

        for cmdstat_type, metrics in cmdstat_dict.items():
            tags = dict(cmd=cmdstat_type, **self.global_tags)
            yield (self.add_metric_prefix('commands_total'), 'g', metrics['calls'], tags)
            yield (self.add_metric_prefix('commands_microsec_total'), 'g', metrics['usec'], tags)

    def send_metrics(self, statsd):
        for metrics in self.cmdstat_metrics():
            statsd.send_metric(*metrics)

        for metrics in self.guage_metrics():
            statsd.send_metric(*metrics)


def main():
    args = parse_args()

    redis_info_buffer = redis_get_info(*(args.redis_host.split(":", 1)))
    redis_stats = RedisStats(redis_info_buffer, args.global_tags, args.prefix)

    statsd = StatsD(*(args.statsd_host.split(":", 1)))
    redis_stats.send_metrics(statsd)


if __name__ == '__main__':
    main()
