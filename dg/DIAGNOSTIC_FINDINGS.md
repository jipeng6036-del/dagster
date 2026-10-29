# slow1 日志缺失的证据边界

首次有效 RAM/.tox 候选官方 slow1（完整5case）为4PASS/1FAIL。失败是 `test_launch_assets_config_files` 的第一次 `dg launch --assets my_asset_3 --config config.json`：returncode为0、日志有正确my_asset_3 step和RUN_SUCCESS，但pytest保存的完整subprocess.stderr没有预期CONFIG:7，也没有STEP_START/STEP_SUCCESS等compute capture区段。原始文件没有覆盖。

同正确环境、锁和官方命令的baseline完整5case为5PASS。之后root明确要求的两次独立候选控制也各5PASS：保留原启动观察器46.96秒；不加载启动观察器45.86秒。第二次实际startup probe证明sitecustomizeLoaded=false。两次都保持默认TAIL_WAIT未设置，不加pytest重试，不缩小case，不改测试或断言。XML case身份集合与原slow1五项完全一致。

两个控制共同使用只读外部filesystem观察：inotify加tail目标定位、只读FD跨unlink持有，不读取或消费进程pipe。各保存26个实际.err/.out文件，最终link count均0，均有3个.err含CONFIG:7和STEP_START；各记录11个实际dg launch进程的argv、cwd、当时definitions/config/pyproject字节与foo-bar editable .pth。with/without两次watcher实际CPU分别0.801/0.746秒，wall分别53.346/50.350秒；不是零开销声明。具体原始字节和定位信息在各controls/*/capture-index.json及capture-*.err/out。成功断言证明该两次stderr包含要求的文本，但没有把成功时subprocess.stderr额外注入抓取出来。

同名生产core代码 `dagster/_core/execution/compute_logs.py` 的 execute_posix_tail 使用tail -F -c +0，并按默认0秒等待立即terminate；原注释明确记录默认1秒检查间隔可能漏stdout/stderr及issue23336。这与缺整段compute日志的形态吻合，但本次原失败的.err临时文件未在首次运行保留，不能用该线索冒充原case直接根因证明。

按root要求只做一次的真实默认机制诊断另存父目录diagnostic-tail-default-once-20261029：专用Docker network=none，实际未改的mirror_stream_to_file包裹一次快速subprocess os.write stderr固定marker，无mock/人为延迟/重试。该次marker在实际.err和subprocess.stderr均出现、exit0，因此为NOT_REPRODUCED_ON_SINGLE_DEFAULT_PROBE，没有循环刷复现。core文件base9f和candidate Git blob均83b298b1fb57c3e6d4afcf0834b99520c732ff3d，实际源码前后SHA均f7048d5814d76400e9853aef5841d5d9bb7e4afc9541f838e41437f376309ef4。

结论仅限：七个官方job现在各有完整候选scope实际通过执行；最初有效batch的slow1失败仍保留。两次控制与一次默认机制诊断没有复现该缺失，故不把它直接标成已证明的flake、I/O根因或本题产品回归，也不宣称已恢复原失败capture。资格与对外正文限制由root基于全部原始证据决定。
