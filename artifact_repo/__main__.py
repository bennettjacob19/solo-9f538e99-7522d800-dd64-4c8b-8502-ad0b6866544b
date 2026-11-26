"""Entry point: ``python -m artifact_repo``."""

from __future__ import annotations

import argparse
import logging
import os

from .store import Repo
from .web import create_app

log = logging.getLogger("artifact_repo")


def main() -> None:
    parser = argparse.ArgumentParser(description="本地制品库（分片上传 + 区间友好发布）")
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("ARTIFACT_DATA_DIR", "./data"),
        help="正文与 SQLite 的存放目录（默认 ./data）",
    )
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("PORT", "8000"))
    )
    parser.add_argument(
        "--server",
        choices=("auto", "flask", "waitress"),
        default=os.environ.get("ARTIFACT_SERVER", "auto"),
        help="auto：装了 waitress 就用它，否则用 Flask 开发服务器",
    )
    parser.add_argument(
        "--simulate-write-failure",
        action="store_true",
        default=os.environ.get("ARTIFACT_SIMULATE_WRITE_FAILURE") == "1",
        help="让每次出版的合并写盘在写完一半时失败（演练回滚）",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    repo = Repo(args.data_dir)
    app = create_app(repo, simulate_write_failure=args.simulate_write_failure)

    server = args.server
    if server == "auto":
        from importlib.util import find_spec

        server = "waitress" if find_spec("waitress") else "flask"

    # A single process is deliberate: publications are serialised in-process
    # and releases are pinned/unpinned in memory.  Multi-process deployments
    # would need an external publish lock and sticky upload routing.
    if server == "waitress":
        from waitress import serve

        log.info("serving with waitress on %s:%s", args.host, args.port)
        serve(app, host=args.host, port=args.port, threads=8, ident="artifact-repo")
    else:
        log.warning("using the Flask development server; install waitress for production")
        app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()
