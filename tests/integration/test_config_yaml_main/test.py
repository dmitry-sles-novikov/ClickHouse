from helpers.cluster import ClickHouseCluster


def test_yaml_main_conf():
    # main configs are in YAML; config.d and users.d are in XML
    cluster = ClickHouseCluster(
        __file__, zookeeper_config_path="configs/config.d/zookeeper.xml"
    )

    all_confd = [
        "configs/config.d/0_common_instance_config.yaml",
        "configs/config.d/access_control.xml",
        "configs/config.d/error_log.xml",
        "configs/config.d/keeper_port.xml",
        "configs/config.d/latency_log.xml",
        "configs/config.d/logging_no_rotate.xml",
        "configs/config.d/log_to_console.xml",
        "configs/config.d/macros.xml",
        "configs/config.d/metric_log.xml",
        "configs/config.d/more_clusters.xml",
        "configs/config.d/part_log.xml",
        "configs/config.d/path.xml",
        "configs/config.d/query_masking_rules.xml",
        "configs/config.d/query_metric_log.xml",
        "configs/config.d/tcp_with_proxy.xml",
        "configs/config.d/test_cluster_with_incorrect_pw.xml",
        "configs/config.d/text_log.xml",
        "configs/config.d/zookeeper.xml",
    ]

    all_userd = [
        "configs/users.d/allow_introspection_functions.xml",
        "configs/users.d/log_queries.xml",
    ]

    node = cluster.add_instance(
        "node",
        base_config_dir="configs",
        main_configs=all_confd,
        user_configs=all_userd,
        with_zookeeper=False,
        main_config_name="config.yaml",
        users_config_name="users.yaml",
        copy_common_configs=False,
        config_root_name="clickhouse",
    )

    try:
        cluster.start()
        assert (
            node.query(
                "select value from system.settings where name = 'max_memory_usage'"
            )
            == "10000000000\n"
        )
        assert (
            node.query(
                "select value from system.settings where name = 'max_block_size'"
            )
            == "64999\n"
        )

    finally:
        cluster.shutdown()
