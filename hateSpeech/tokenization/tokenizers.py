from abc import ABC, abstractmethod

import os
from typing import Optional, Union
from tempfile import TemporaryDirectory

from tokenizers import Tokenizer
from tokenizers.decoders import Decoder
from tokenizers.models import Model
from tokenizers.normalizers import Normalizer
from tokenizers.pre_tokenizers import PreTokenizer
from tokenizers.trainers import BpeTrainer, UnigramTrainer, WordPieceTrainer, WordLevelTrainer
from tokenizers.processors import BertProcessing, ByteLevel, RobertaProcessing, TemplateProcessing
from transformers import PreTrainedTokenizerFast
from hateSpeech.utils.io_utils import copy_dir

TrainerType = Union[BpeTrainer, UnigramTrainer, WordPieceTrainer, WordLevelTrainer]
PostProcessorType = Union[BertProcessing, ByteLevel, RobertaProcessing, TemplateProcessing]

class TokenizerBase(ABC):
    @abstractmethod
    def train(self, texts: list[str]) -> None:
        ...

    @abstractmethod
    def save(self, tokenizer_dir: str) -> None:
        ...

class HuggingFaceTokenizer(TokenizerBase):
    def __init__(
            self,
            pre_tokenizer: PreTokenizer,
            model: Model,
            trainer: TrainerType,
            normalizer: Optional[Normalizer] = None,
            decoder: Optional[Decoder] = None,
            post_processor: Optional[PostProcessorType] = None,
            unk_token: Optional[str] = None,
            cls_token: Optional[str] = None,
            pad_token: Optional[str] = None,
            sep_token: Optional[str] = None,
            mask_token: Optional[str] = None,

    )-> None:
        
        self.unk_token = unk_token
        self.cls_token = cls_token
        self.pad_token = pad_token
        self.sep_token = sep_token
        self.mask_token = mask_token

        self.tokenizer = Tokenizer(model)
        self.tokenizer.pre_tokenizer = pre_tokenizer
        self.trainer = trainer

        if normalizer is not None:
            self.tokenizer.normalizer = normalizer
        
        if decoder is not None:
            self.tokenizer.decoder = decoder

        if post_processor is not None:
            self.tokenizer.post_processor = post_processor
        
    def train(self, texts: list[str]) -> None:
        self.tokenizer.train_from_iterator(texts, trainer=self.trainer)
        if self.pad_token is not None:
            self.tokenizer.enable_padding(pad_id=self.tokenizer.token_to_id(self.pad_token), pad_token=self.pad_token)

        
    
    def save(self, tokenizer_save_dir: str) -> None:
        tokenizer = PreTrainedTokenizerFast(tokenizer_object=self.tokenizer, unk_token=self.unk_token, cls_token=self.cls_token, pad_token=self.pad_token, sep_token=self.sep_token, mask_token=self.mask_token)
        with TemporaryDirectory() as temp_dir:
            temp_tokenizer_save_path = os.path.join(temp_dir, "trained_tokenizer")
            tokenizer.save_pretrained(temp_tokenizer_save_path)
            copy_dir(temp_tokenizer_save_path, tokenizer_save_dir)
