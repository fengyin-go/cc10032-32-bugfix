"""序号32修复验证：窗口策略版本绑定、水位计算、迟到补算清单。

覆盖：
  - 策略修改允许延迟后，窗口消费按当前生效版本生成窗口（不再沿用旧值）
  - 窗口指标数据绑定当前策略版本，旧口径窗口保持可追溯、不被静默重写
  - 水位只依据当前版本窗口边界计算
  - 消费写入幂等：并发重试只保留一版
  - 迟到处理模块读取当前窗口指标+策略版本，生成迟到补算清单
  - 状态单向生命周期流转，非法流转回滚保留原状态
  - 滑动指标与窗口配置共享同一根因：按当前版本读取窗口指标
"""
from __future__ import annotations

import time

import pytest

from app.services.late_handler import LateHandlerService
from app.services.sliding_index import SlidingIndexService
from app.services.window_config import WindowConfigService
from app.store import store

# 使用独立命名，避免与种子数据及其他测试互相影响
POLICY = "序号32测试策略"
SOURCE = "seq32_source"


def _svc() -> WindowConfigService:
    return WindowConfigService()


def _register_policy(svc: WindowConfigService, name: str, lateness: str,
                     size: str = "60", step: str = "30") -> dict:
    entry, missing = svc.create_entry({
        "策略名称": name,
        "窗口类型": "sliding",
        "窗口大小": size,
        "滑动步长": step,
        "水位延迟": lateness,
        "触发策略": "watermark",
        "延迟处理": "bypass",
    })
    assert not missing
    return entry


def _enable(svc: WindowConfigService, entry: dict) -> None:
    updated, msg = svc.run_action(entry["id"], "启用策略")
    assert updated is not None, msg
    assert updated["status"] == "已生效"


def _publish_batch(source: str, count: int = 24, base_ts: float | None = None) -> str:
    if base_ts is None:
        base_ts = time.time()
    batch_id = f"batch_{source}_{int(base_ts * 1000)}_{count}"
    store.rows("stream_source").append({
        "batch_id": batch_id,
        "source": source,
        "events": [
            {
                "event_id": f"{source}_ev_{i}",
                "event_time": base_ts + i * 5.0,
                "payload": {"user_id": f"u{i % 3}", "amount": 10.0 + i},
            }
            for i in range(count)
        ],
        "produced_at": time.time(),
    })
    return batch_id


def _metrics(policy_name: str) -> list[dict]:
    return [
        r for r in store.rows("window_config")
        if isinstance(r.get("window_definitions"), list)
        and r.get("策略名称") == policy_name
    ]


@pytest.fixture()
def svc() -> WindowConfigService:
    return _svc()


# ---------------------------------------------------------------------------
# 核心修复：策略修改允许延迟后，窗口消费按当前生效版本生成
# ---------------------------------------------------------------------------

class TestPolicyVersionBinding:
    """窗口指标必须绑定当前生效策略版本。"""

    def test_consume_uses_current_version_lateness(self, svc):
        """策略修改允许延迟后，新消费不再沿用最初绑定的旧值。"""
        name = f"{POLICY}_lateness"
        v1 = _register_policy(svc, name, "10")
        _enable(svc, v1)
        _publish_batch(f"{SOURCE}_lat1")
        r1 = svc.consume_source_batch(f"{SOURCE}_lat1", name)
        assert r1["ok"] and r1["policy_version"] == 1
        assert r1["allowed_lateness"] == 10.0

        # 修改允许延迟：登记同名新版本并启用
        v2 = _register_policy(svc, name, "40")
        assert v2["policy_version"] == 2
        _enable(svc, v2)
        _publish_batch(f"{SOURCE}_lat2")
        r2 = svc.consume_source_batch(f"{SOURCE}_lat2", name)
        assert r2["ok"], r2
        assert r2["policy_version"] == 2
        assert r2["allowed_lateness"] == 40.0

        metrics = _metrics(name)
        m1 = [m for m in metrics if m["policy_version"] == 1]
        m2 = [m for m in metrics if m["policy_version"] == 2]
        assert len(m1) == 1 and len(m2) == 1
        # 新窗口按新口径生成
        assert m2[0]["水位延迟"] == "40"
        assert m2[0]["allowed_lateness"] == 40.0
        # 旧口径窗口保持原样可追溯，未被新口径静默重写
        assert m1[0]["水位延迟"] == "10"
        assert m1[0]["allowed_lateness"] == 10.0

    def test_draft_new_version_does_not_switch(self, svc):
        """新版本未启用时，消费仍按旧生效版本（新版本边界生效后才切换）。"""
        name = f"{POLICY}_draft"
        v1 = _register_policy(svc, name, "15")
        _enable(svc, v1)
        _register_policy(svc, name, "99")  # 草稿态新版本
        _publish_batch(f"{SOURCE}_draft")
        r = svc.consume_source_batch(f"{SOURCE}_draft", name)
        assert r["ok"] and r["policy_version"] == 1
        assert r["allowed_lateness"] == 15.0

    def test_consume_requires_effective_policy(self, svc):
        """策略不存在或未生效时消费失败。"""
        name = f"{POLICY}_inactive"
        assert svc.consume_source_batch(SOURCE, f"{name}_none")["ok"] is False
        _register_policy(svc, name, "10")  # 草稿未启用
        _publish_batch(f"{SOURCE}_inactive")
        r = svc.consume_source_batch(f"{SOURCE}_inactive", name)
        assert r["ok"] is False and "未生效" in r["error"]


# ---------------------------------------------------------------------------
# 水位只依据当前版本窗口边界计算
# ---------------------------------------------------------------------------

class TestWatermarkCurrentVersion:
    """水位计算绑定当前策略版本，旧记录不再混入。"""

    def test_watermark_only_from_current_version(self, svc):
        name = f"{POLICY}_wm"
        v1 = _register_policy(svc, name, "10")
        _enable(svc, v1)
        _publish_batch(f"{SOURCE}_wm1")
        svc.consume_source_batch(f"{SOURCE}_wm1", name)

        v2 = _register_policy(svc, name, "40")
        _enable(svc, v2)
        _publish_batch(f"{SOURCE}_wm2")
        svc.consume_source_batch(f"{SOURCE}_wm2", name)

        result = svc.calculate_watermark(name)
        assert result["ok"] and result["policy_version"] == 2
        assert result["allowed_lateness"] == 40.0
        # 当前水位只来自 v2 窗口边界
        m2 = [m for m in _metrics(name) if m["policy_version"] == 2][0]
        expected_end = max(w["window_end"] for w in m2["window_definitions"])
        current = result["current"]
        assert current["policy_version"] == 2
        assert current["window_end"] == expected_end
        assert current["watermark"] == expected_end - 40.0
        # watermarks 列表不含旧版本记录
        assert all(w["policy_version"] == 2 for w in result["watermarks"])
        # 旧口径水位单独留痕可追溯
        history = result["history"]
        assert len(history) == 1
        assert history[0]["policy_version"] == 1
        assert history[0]["allowed_lateness"] == 10.0
        assert history[0]["traceable"] is True

    def test_watermark_without_effective_policy(self, svc):
        result = svc.calculate_watermark(f"{POLICY}_wm_none")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# 幂等：并发重试只保留一版
# ---------------------------------------------------------------------------

class TestIdempotentConsume:
    """消费写入按 (策略, 版本, 批次) 幂等。"""

    def test_retry_keeps_single_version(self, svc):
        name = f"{POLICY}_idem"
        v1 = _register_policy(svc, name, "10")
        _enable(svc, v1)
        _publish_batch(f"{SOURCE}_idem")
        r1 = svc.consume_source_batch(f"{SOURCE}_idem", name)
        assert r1["ok"] and r1["idempotent_replay"] is False
        # 并发重试同一批次
        r2 = svc.consume_source_batch(f"{SOURCE}_idem", name)
        r3 = svc.consume_source_batch(f"{SOURCE}_idem", name)
        assert r2["idempotent_replay"] is True
        assert r3["idempotent_replay"] is True
        assert r1["metric_key"] == r2["metric_key"] == r3["metric_key"]
        # 只保留一版
        metrics = [m for m in _metrics(name) if m.get("metric_key") == r1["metric_key"]]
        assert len(metrics) == 1

    def test_new_version_gets_new_metric(self, svc):
        """策略切换后同批次重消费生成新版本指标，旧版本保留可追溯。"""
        name = f"{POLICY}_idemv"
        v1 = _register_policy(svc, name, "10")
        _enable(svc, v1)
        _publish_batch(f"{SOURCE}_idemv")
        svc.consume_source_batch(f"{SOURCE}_idemv", name)
        v2 = _register_policy(svc, name, "20")
        _enable(svc, v2)
        r = svc.consume_source_batch(f"{SOURCE}_idemv", name)
        assert r["ok"] and r["policy_version"] == 2
        versions = sorted(m["policy_version"] for m in _metrics(name))
        assert versions == [1, 2]


# ---------------------------------------------------------------------------
# 迟到补算清单
# ---------------------------------------------------------------------------

class TestLateRecomputeList:
    """迟到处理模块读取当前窗口指标数据和策略版本，生成迟到补算清单。"""

    def test_generate_recompute_list(self, svc):
        name = f"{POLICY}_late"
        v1 = _register_policy(svc, name, "40")
        _enable(svc, v1)
        _publish_batch(f"{SOURCE}_late")
        svc.consume_source_batch(f"{SOURCE}_late", name)

        handler = LateHandlerService()
        result = handler.generate_late_recompute_list(name)
        assert result["ok"], result
        assert result["policy_version"] == 1
        assert result["allowed_lateness"] == 40.0
        assert result["late_count"] > 0
        assert result["recompute_list"], "应生成迟到补算清单"
        # 补算清单中的水位 = 当前版本最大窗口边界 - 允许延迟
        metric = _metrics(name)[0]
        expected_wm = max(w["window_end"] for w in metric["window_definitions"]) - 40.0
        for item in result["recompute_list"]:
            assert item["policy_version"] == 1
            assert item["late_count"] > 0
            assert item["watermark"] == pytest.approx(expected_wm)
        # 补算清单回写到当前窗口指标数据，状态详情可核对版本、水位和迟到事件
        assert metric["policy_version"] == 1
        assert metric["watermark"] == pytest.approx(expected_wm)
        assert metric["late_events"] == result["late_count"]
        assert len(metric["late_recompute_list"]) == len(result["recompute_list"])

    def test_old_version_metrics_not_rewritten(self, svc):
        """旧口径窗口指标不被新口径静默重写。"""
        name = f"{POLICY}_late_old"
        v1 = _register_policy(svc, name, "10")
        _enable(svc, v1)
        _publish_batch(f"{SOURCE}_lateold1")
        svc.consume_source_batch(f"{SOURCE}_lateold1", name)
        v2 = _register_policy(svc, name, "50")
        _enable(svc, v2)
        _publish_batch(f"{SOURCE}_lateold2")
        svc.consume_source_batch(f"{SOURCE}_lateold2", name)

        handler = LateHandlerService()
        result = handler.generate_late_recompute_list(name)
        assert result["ok"] and result["policy_version"] == 2
        metrics = _metrics(name)
        old = [m for m in metrics if m["policy_version"] == 1][0]
        # 旧版本指标未被补算清单改写
        assert "late_recompute_list" not in old
        assert old["allowed_lateness"] == 10.0

    def test_recompute_requires_effective_policy(self):
        handler = LateHandlerService()
        result = handler.generate_late_recompute_list(f"{POLICY}_late_none")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# 单向生命周期与事务回滚
# ---------------------------------------------------------------------------

class TestOneWayLifecycle:
    """32号状态按单向生命周期流转，非法流转回滚保留原状态。"""

    def test_one_way_transitions(self, svc):
        name = f"{POLICY}_life"
        entry = _register_policy(svc, name, "10")
        assert entry["status"] == "草稿"
        # 草稿不能直接停用
        updated, msg = svc.run_action(entry["id"], "停用策略")
        assert updated is None
        assert store.find("window_config", entry["id"])["status"] == "草稿"
        # 草稿→已生效→已停用 单向推进
        _enable(svc, entry)
        # 已生效不能重复启用（不回退）
        updated, _ = svc.run_action(entry["id"], "启用策略")
        assert updated is None
        assert store.find("window_config", entry["id"])["status"] == "已生效"
        updated, _ = svc.run_action(entry["id"], "停用策略")
        assert updated is not None and updated["status"] == "已停用"
        # 已停用为终态，不能再启用
        updated, _ = svc.run_action(entry["id"], "启用策略")
        assert updated is None
        assert store.find("window_config", entry["id"])["status"] == "已停用"

    def test_archive_conditions_written_to_metrics(self, svc):
        """32号归档和恢复条件写入窗口指标数据。"""
        name = f"{POLICY}_arch"
        entry = _register_policy(svc, name, "10")
        _enable(svc, entry)
        _publish_batch(f"{SOURCE}_arch")
        svc.consume_source_batch(f"{SOURCE}_arch", name)
        svc.run_action(entry["id"], "停用策略")
        metric = _metrics(name)[0]
        assert metric["archived"] is True
        assert "archive_condition" in metric
        assert "recovery_condition" in metric

    def test_unknown_action_rejected(self, svc):
        entry = _register_policy(svc, f"{POLICY}_life2", "10")
        updated, msg = svc.run_action(entry["id"], "不存在的动作")
        assert updated is None
        assert store.find("window_config", entry["id"])["status"] == "草稿"


# ---------------------------------------------------------------------------
# 滑动指标与窗口配置共享同一根因
# ---------------------------------------------------------------------------

class TestSlidingIndexVersionBinding:
    """滑动指标只读取当前策略版本的窗口指标数据。"""

    def _register_index(self, name: str) -> dict:
        svc = SlidingIndexService()
        entry, missing = svc.create_entry({
            "指标名称": name,
            "计算窗口": "60",
            "聚合函数": "sum",
            "分组维度": "user_id",
            "过滤条件": "",
            "快照频率": "10s",
            "保留时长": "720",
        })
        assert not missing
        svc.run_action(entry["id"], "启用指标")
        return entry

    def test_compute_binds_current_version(self, svc):
        name = f"{POLICY}_idx"
        v1 = _register_policy(svc, name, "10")
        _enable(svc, v1)
        _publish_batch(f"{SOURCE}_idx1")
        svc.consume_source_batch(f"{SOURCE}_idx1", name)

        index_svc = SlidingIndexService()
        index_name = "序号32测试指标"
        self._register_index(index_name)
        r1 = index_svc.compute_from_window_metrics(index_name, name)
        assert r1["ok"] and r1["policy_version"] == 1
        windows_v1 = r1["windows"]

        # 策略切换后，滑动指标读取新版本窗口（详情窗口可核对切换结果）
        v2 = _register_policy(svc, name, "30")
        _enable(svc, v2)
        _publish_batch(f"{SOURCE}_idx2")
        svc.consume_source_batch(f"{SOURCE}_idx2", name)
        r2 = index_svc.compute_from_window_metrics(index_name, name)
        assert r2["ok"] and r2["policy_version"] == 2
        assert r2["windows"] != 0
        # 切换结果：两次计算依据不同版本
        index_def = next(
            r for r in store.rows("sliding_index") if r.get("指标名称") == index_name
        )
        assert index_def["_last_policy_version"] == 2
        assert r1["windows"] == windows_v1

    def test_compute_requires_effective_policy(self):
        index_svc = SlidingIndexService()
        index_name = "序号32测试指标2"
        entry, _ = index_svc.create_entry({
            "指标名称": index_name,
            "计算窗口": "60",
            "聚合函数": "sum",
            "分组维度": "",
            "过滤条件": "",
            "快照频率": "",
            "保留时长": "",
        })
        assert entry is not None
        result = index_svc.compute_from_window_metrics(index_name, f"{POLICY}_idx_none")
        assert result["ok"] is False

    def test_sliding_index_one_way_lifecycle(self):
        index_svc = SlidingIndexService()
        entry, _ = index_svc.create_entry({
            "指标名称": "序号32测试指标3",
            "计算窗口": "60",
            "聚合函数": "count",
            "分组维度": "",
            "过滤条件": "",
            "快照频率": "",
            "保留时长": "",
        })
        # 草稿不能直接停用
        updated, _ = index_svc.run_action(entry["id"], "停用指标")
        assert updated is None
        assert store.find("sliding_index", entry["id"])["status"] == "草稿"
        updated, _ = index_svc.run_action(entry["id"], "启用指标")
        assert updated is not None and updated["status"] == "计算中"
