from __future__ import annotations

import threading
from typing import Callable, Iterator

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, StoppingCriteria, StoppingCriteriaList, TextIteratorStreamer

from inference_worker.backends.base import GeneratedToken
from inference_worker.events import SamplingOptions


class CancellationStoppingCriteria(StoppingCriteria):
    def __init__(self, should_cancel: Callable[[], bool]) -> None:
        self.should_cancel = should_cancel

    def __call__(self, input_ids: torch.LongTensor, scores: torch.FloatTensor, **kwargs: object) -> bool:
        del input_ids, scores, kwargs
        return self.should_cancel()


class TransformersBackend:
    def __init__(self, *, model_name: str, device: str, dtype: str) -> None:
        torch_dtype = _resolve_dtype(dtype)
        device_map = "auto" if device == "auto" else {"": device}
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch_dtype,
            device_map=device_map,
        )
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

    def generate(
        self,
        *,
        prompt: str,
        max_tokens: int,
        sampling: SamplingOptions,
        should_cancel: Callable[[], bool],
    ) -> Iterator[GeneratedToken]:
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        streamer = TextIteratorStreamer(
            self.tokenizer,
            skip_prompt=True,
            skip_special_tokens=True,
        )
        generation_kwargs = {
            **inputs,
            "max_new_tokens": max_tokens,
            "do_sample": sampling.temperature > 0,
            "temperature": max(sampling.temperature, 0.01),
            "top_p": sampling.top_p,
            "top_k": sampling.top_k,
            "streamer": streamer,
            "stopping_criteria": StoppingCriteriaList([CancellationStoppingCriteria(should_cancel)]),
            "pad_token_id": self.tokenizer.pad_token_id,
        }
        thread = threading.Thread(target=self.model.generate, kwargs=generation_kwargs, daemon=True)
        thread.start()
        for text in streamer:
            if should_cancel():
                break
            yield GeneratedToken(text=text, probability=0.0)
        thread.join(timeout=1)


def _resolve_dtype(dtype: str) -> torch.dtype:
    if dtype == "float32":
        return torch.float32
    if dtype == "bfloat16":
        return torch.bfloat16
    return torch.float16
