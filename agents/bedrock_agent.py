from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0
THROTTLE_CODES = {"ThrottlingException", "TooManyRequestsException", "ServiceUnavailableException"}


class BedrockAgent:
    """Wrapper around Amazon Bedrock runtime for Claude model invocation with retries and rate-limit handling."""

    def __init__(
        self,
        model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0",
        region: str = "us-east-1",
        max_retries: int = MAX_RETRIES,
    ):
        self.model_id = model_id
        self.region = region
        self.max_retries = max_retries
        self._client = boto3.client("bedrock-runtime", region_name=region)

    def invoke(
        self,
        prompt: str,
        system_prompt: str | None = None,
        max_tokens: int = 4096,
    ) -> str:
        body = self._build_body(prompt, system_prompt, max_tokens)
        response = self._call_with_retries(body)
        return self._extract_text(response)

    def invoke_with_tools(
        self,
        prompt: str,
        tools: list[dict],
        system_prompt: str | None = None,
        max_tokens: int = 4096,
    ) -> dict:
        body = self._build_body(prompt, system_prompt, max_tokens, tools=tools)
        response = self._call_with_retries(body)
        return self._parse_tool_response(response)

    def invoke_streaming(
        self,
        prompt: str,
        system_prompt: str | None = None,
        max_tokens: int = 4096,
    ):
        body = self._build_body(prompt, system_prompt, max_tokens)

        response = self._client.invoke_model_with_response_stream(
            modelId=self.model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body),
        )

        for event in response["body"]:
            chunk = json.loads(event["chunk"]["bytes"])
            if chunk.get("type") == "content_block_delta":
                delta = chunk.get("delta", {})
                if delta.get("type") == "text_delta":
                    yield delta["text"]

    # -- internal helpers --

    @staticmethod
    def _build_body(
        prompt: str,
        system_prompt: str | None,
        max_tokens: int,
        tools: list[dict] | None = None,
    ) -> dict:
        body: dict = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system_prompt:
            body["system"] = system_prompt
        if tools:
            body["tools"] = tools
        return body

    def _call_with_retries(self, body: dict) -> dict:
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.invoke_model(
                    modelId=self.model_id,
                    contentType="application/json",
                    accept="application/json",
                    body=json.dumps(body),
                )
                return json.loads(response["body"].read())
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                last_error = e
                if error_code in THROTTLE_CODES and attempt < self.max_retries:
                    delay = RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        "Rate limited (attempt %d/%d), retrying in %.1fs",
                        attempt + 1,
                        self.max_retries,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    raise
        raise last_error  # type: ignore[misc]

    @staticmethod
    def _extract_text(response: dict) -> str:
        content = response.get("content", [])
        text_parts = [
            block["text"] for block in content if block.get("type") == "text"
        ]
        return "\n".join(text_parts)

    @staticmethod
    def _parse_tool_response(response: dict) -> dict:
        content = response.get("content", [])
        result: dict = {
            "text": "",
            "tool_calls": [],
            "stop_reason": response.get("stop_reason", ""),
        }

        for block in content:
            if block.get("type") == "text":
                result["text"] += block["text"]
            elif block.get("type") == "tool_use":
                result["tool_calls"].append(
                    {
                        "id": block["id"],
                        "name": block["name"],
                        "input": block["input"],
                    }
                )

        return result
