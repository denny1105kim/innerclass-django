from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import redis

@dataclass(frozen=True)
class QueueConfig:
    redis_url: str
    in_queue: str


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def get_queue_config() -> QueueConfig:
    """
    Required env:
      - REDIS_URL   (ex: redis://redis:6379/0)
      - IN_QUEUE    (ex: news:queue)

    Optional env:
      - REDIS_SOCKET_TIMEOUT_SEC (default: 3)
    """
    redis_url = _env("REDIS_URL", "redis://redis:6379/0")
    in_queue = _env("IN_QUEUE", "news:queue")

    return QueueConfig(redis_url=redis_url, in_queue=in_queue)


def _redis_client(redis_url: str) -> redis.Redis:

    socket_timeout = float(_env("REDIS_SOCKET_TIMEOUT_SEC", "3") or "3")
    return redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_timeout,
        retry_on_timeout=True,
    )


def build_job_payload(
    *,
    article_id: int,
    title: str,
    content: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:

    job: Dict[str, Any] = {
        "article_id": int(article_id),
        "title": (title or "").strip(),
        "content": (content or "").strip(),
    }
    if extra:
        # 충돌 방지: 기본 키는 덮지 않음
        for k, v in extra.items():
            if k in job:
                continue
            job[k] = v
    return job


def enqueue_article_for_classify(
    *,
    article_id: int,
    title: str,
    content: str,
    redis_url: Optional[str] = None,
    queue_name: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Redis 리스트(큐)에 classification job을 넣습니다. (LPUSH)

    Returns:
      - enqueue 후 리스트 길이 (redis lpush return)
    Raises:
      - ValueError: 필수 값이 비었을 때
      - redis.RedisError: Redis 장애/연결 실패 등
    """
    title = (title or "").strip()
    content = (content or "").strip()

    if not article_id:
        raise ValueError("article_id is required")
    if not title and not content:
        raise ValueError("title/content are empty; nothing to enqueue")

    cfg = get_queue_config()
    rurl = (redis_url or cfg.redis_url).strip()
    qname = (queue_name or cfg.in_queue).strip()

    if not rurl:
        raise ValueError("REDIS_URL is empty (env or argument)")
    if not qname:
        raise ValueError("IN_QUEUE is empty (env or argument)")

    job = build_job_payload(article_id=article_id, title=title, content=content, extra=extra)
    raw = json.dumps(job, ensure_ascii=False, separators=(",", ":"))

    r = _redis_client(rurl)
    # LPUSH: worker는 BRPOP으로 꺼내므로 "오른쪽 pop" 기준으로 왼쪽 push가 일반적 조합입니다.
    return int(r.lpush(qname, raw))


def ping_redis(redis_url: Optional[str] = None) -> bool:
    """
    헬스체크/디버깅용.
    """
    cfg = get_queue_config()
    rurl = (redis_url or cfg.redis_url).strip()
    if not rurl:
        return False
    r = _redis_client(rurl)
    return bool(r.ping())


def get_queue_length(queue_name: Optional[str] = None, redis_url: Optional[str] = None) -> int:
    """
    디버깅용: 현재 큐 길이 확인
    """
    cfg = get_queue_config()
    rurl = (redis_url or cfg.redis_url).strip()
    qname = (queue_name or cfg.in_queue).strip()
    r = _redis_client(rurl)
    return int(r.llen(qname))
