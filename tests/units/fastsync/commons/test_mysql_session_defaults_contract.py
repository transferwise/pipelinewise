"""Keep MySQL session defaults aligned without importing connector runtimes."""

import ast
from pathlib import Path


def _session_defaults(path):
    module = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    assignments = {
        target.id: node.value
        for node in module.body if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
    }

    def resolve(node):
        if isinstance(node, ast.Name):
            return resolve(assignments[node.id])
        if isinstance(node, ast.List):
            return [resolve(element) for element in node.elts]
        return ast.literal_eval(node)

    return {
        name: resolve(assignments[name])
        for name in (
            'DEFAULT_SESSION_SQLS',
            'DEFAULT_NET_WRITE_TIMEOUT_SQL',
            'MARIADB_MAX_STATEMENT_TIME_SQL',
            'MYSQL_MAX_EXECUTION_TIME_SQL',
        )
    }


def test_mysql_session_defaults_match_between_fastsync_and_singer():
    repo_root = Path(__file__).resolve().parents[4]
    fastsync_defaults = _session_defaults(repo_root / 'pipelinewise/fastsync/commons/tap_mysql.py')
    singer_defaults = _session_defaults(repo_root / 'singer-connectors/tap-mysql/tap_mysql/connection.py')

    assert fastsync_defaults == singer_defaults
