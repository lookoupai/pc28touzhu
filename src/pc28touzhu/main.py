"""Entrypoint for running the minimal WSGI app."""
from __future__ import annotations

from wsgiref.simple_server import make_server

from pc28touzhu.config import get_runtime_config


def create_app(repository):
    from pc28touzhu.api.app import PlatformApiApplication

    config = get_runtime_config()
    return PlatformApiApplication(
        repository=repository,
        executor_api_token=config.platform.executor_api_token,
        session_secret=config.platform.session_secret,
        platform_config=config.platform,
        telegram_bot_config=config.telegram_bot,
        runtime_config=config,
    )


def run_server(repository):
    config = get_runtime_config()
    app = create_app(repository)
    host = config.platform.host
    port = config.platform.port
    httpd = make_server(host, port, app)
    print("platform listening on %s:%s" % (host, port))
    httpd.serve_forever()


def build_repository(db_path: str | None = None):
    from pc28touzhu.executor.db_repository import DatabaseRepository

    config = get_runtime_config()
    resolved_db_path = db_path or config.platform.database_path
    db_parent = __import__("pathlib").Path(resolved_db_path).expanduser().resolve().parent
    db_parent.mkdir(parents=True, exist_ok=True)
    repo = DatabaseRepository(resolved_db_path)
    repo.initialize_database()
    return repo


def main():
    repo = build_repository()
    run_server(repo)


if __name__ == "__main__":
    main()
