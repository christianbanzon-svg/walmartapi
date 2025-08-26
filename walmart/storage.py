import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from config import get_config


@dataclass
class ListingSnapshot:
	listing_id: str
	data_json: str
	created_at: str


@dataclass
class SellerSnapshot:
	listing_id: str
	seller_id: str
	data_json: str
	created_at: str


def _utc_now_iso() -> str:
	return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


@contextmanager
def connect_db():
	cfg = get_config()
	conn = sqlite3.connect(cfg.database_path)
	conn.execute("PRAGMA journal_mode=WAL;")
	try:
		yield conn
	finally:
		conn.commit()
		conn.close()


def init_db() -> None:
	with connect_db() as conn:
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS listings (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				listing_id TEXT NOT NULL,
				title TEXT,
				brand TEXT,
				url TEXT,
				last_seen_at TEXT
			);
			"""
		)
		conn.execute(
			"""
			CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_listing_id ON listings(listing_id);
			"""
		)
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS listing_history (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				listing_id TEXT NOT NULL,
				data_json TEXT NOT NULL,
				created_at TEXT NOT NULL
			);
			"""
		)
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS seller_history (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				listing_id TEXT NOT NULL,
				seller_id TEXT NOT NULL,
				data_json TEXT NOT NULL,
				created_at TEXT NOT NULL
			);
			"""
		)


def upsert_listing_summary(listing_id: str, title: Optional[str], brand: Optional[str], url: Optional[str]) -> None:
	with connect_db() as conn:
		conn.execute(
			"""
			INSERT INTO listings (listing_id, title, brand, url, last_seen_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(listing_id) DO UPDATE SET
				title=excluded.title,
				brand=excluded.brand,
				url=excluded.url,
				last_seen_at=excluded.last_seen_at
			""",
			(listing_id, title, brand, url, _utc_now_iso()),
		)


def insert_listing_snapshot(listing_id: str, data: Dict[str, Any]) -> None:
	with connect_db() as conn:
		conn.execute(
			"""
			INSERT INTO listing_history (listing_id, data_json, created_at)
			VALUES (?, ?, ?)
			""",
			(listing_id, json.dumps(data, ensure_ascii=False), _utc_now_iso()),
		)


def insert_seller_snapshot(listing_id: str, seller_id: str, data: Dict[str, Any]) -> None:
	with connect_db() as conn:
		conn.execute(
			"""
			INSERT INTO seller_history (listing_id, seller_id, data_json, created_at)
			VALUES (?, ?, ?, ?)
			""",
			(listing_id, seller_id, json.dumps(data, ensure_ascii=False), _utc_now_iso()),
		)


