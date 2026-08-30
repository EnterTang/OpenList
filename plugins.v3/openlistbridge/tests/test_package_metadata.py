import json
from pathlib import Path
import unittest


class PackageMetadataTest(unittest.TestCase):
    def test_v3_repository_index_matches_bridge_plugin(self):
        repository_root = Path(__file__).parents[2]
        metadata = json.loads((repository_root / "package.v3.json").read_text())
        bridge = metadata["OpenListBridge"]

        self.assertEqual(bridge["version"], "1.0.0")
        self.assertEqual(bridge["system_version"], ">=3.0.0")
        self.assertEqual(bridge["icon"], "OpenList.png")
        self.assertEqual(bridge["history"]["v1.0.0"],
                         "首个 MoviePilot V3 版本：提供 OpenList 资源搜索、下载意图和精确 qB 状态回传。")


if __name__ == "__main__":
    unittest.main()
