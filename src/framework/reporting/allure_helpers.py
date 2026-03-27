from __future__ import annotations

import json

import allure
import pandas as pd


def attach_json(name: str, payload: dict | list) -> None:
    """Attach a JSON payload to Allure."""
    allure.attach(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        name=name,
        attachment_type=allure.attachment_type.JSON,
    )


def attach_dataframe(name: str, dataframe: pd.DataFrame) -> None:
    """Attach a dataframe to Allure as CSV."""
    allure.attach(
        dataframe.to_csv(index=False),
        name=name,
        attachment_type=allure.attachment_type.CSV,
    )


def attach_text(name: str, content: str) -> None:
    """Attach plain text to Allure."""
    allure.attach(content, name=name, attachment_type=allure.attachment_type.TEXT)
