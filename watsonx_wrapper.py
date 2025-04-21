"""
Module for IBM WatsonX AI model inference.

This module provides a wrapper class to interact with IBM WatsonX AI for text generation.
It loads credentials from environment variables and supports both synchronous and streaming
text generation using predefined or custom parameters.
"""

import os
from dotenv import load_dotenv
from ibm_watsonx_ai import Credentials
from ibm_watsonx_ai.foundation_models.schema import TextGenParameters, TextChatParameters
from ibm_watsonx_ai.foundation_models import ModelInference
from typing import Tuple

class WatsonxWrapper:
    """
    A wrapper class for interacting with IBM WatsonX AI model inference.
    """

    def __init__(self, model_id: str = "meta-llama/llama-3-1-70b-instruct",
                 params: TextGenParameters = None) -> None:
        """
        Initialize the WatsonxWrapper with model credentials and parameters.

        Args:
            model_id (str): The model ID for inference.
            params (TextGenParameters, optional): Custom text generation parameters.
                Defaults to a predefined set of parameters.

        Raises:
            ValueError: If required environment variables are missing.
        """
        load_dotenv()

        self.model_id: str = model_id
        watsonx_endpoint: str = os.getenv("WATSONX_ENDPOINT")
        api_key: str = os.getenv("IBM_CLOUD_API_KEY")
        if not watsonx_endpoint or not api_key:
            raise ValueError("Missing required environment variables: WATSONX_ENDPOINT or IBM_CLOUD_API_KEY.")

        self.credentials: Credentials = Credentials(
            url=watsonx_endpoint,
            api_key=api_key
        )

        self.project_id: str = os.getenv("WATSONX_PROJECT_ID")
        if not self.project_id:
            raise ValueError("Project ID not found in environment variables.")

        self.params: TextGenParameters = params or TextGenParameters(
            temperature=0,
            max_new_tokens=1000,
            random_seed=42,
            decoding_method='greedy',
            min_new_tokens=1
        )

        self.model: ModelInference = ModelInference(
            model_id=self.model_id,
            credentials=self.credentials,
            project_id=self.project_id,
            params=self.params
        )

    def generate_text_with_token_count(self, prompt: str, params: TextGenParameters = None) -> Tuple[str,str,str]:
        """
        Generate text synchronously based on a given prompt.

        Args:
            prompt (str): The input prompt for text generation.
            params (TextGenParameters, optional): Custom parameters for generation.
                Defaults to the instance's parameters.

        Returns:
            str: The generated text.
        """
        full_llm_response = self.model.generate_text(prompt=prompt, params=params or self.params,raw_response=True)
        # print(full_llm_response)
        # print('generated_token_count',full_llm_response["results"][0]['generated_token_count'])
        # print('input_token_count',full_llm_response["results"][0]['input_token_count'])
        return full_llm_response["results"][0]["generated_text"], full_llm_response["results"][0]['generated_token_count'], full_llm_response["results"][0]['input_token_count']
    def generate_text(self, prompt: str, params: TextGenParameters = None) -> str:
        """
        Generate text synchronously based on a given prompt.

        Args:
            prompt (str): The input prompt for text generation.
            params (TextGenParameters, optional): Custom parameters for generation.
                Defaults to the instance's parameters.

        Returns:
            str: The generated text.
        """
        llm_response = self.model.generate_text(prompt=prompt, params=params or self.params)
        return llm_response

    def generate_text_stream(self, prompt: str, params: TextGenParameters = None):
        """
        Generate text as a stream based on a given prompt.

        Args:
            prompt (str): The input prompt for text generation.
            params (TextGenParameters, optional): Custom parameters for generation.
                Defaults to the instance's parameters.

        Returns:
            Iterator: An iterator over the generated text tokens.
        """
        return self.model.generate_text_stream(prompt=prompt, params=params or self.params)


if __name__ == "__main__":
    wxw: WatsonxWrapper = WatsonxWrapper()
    print(wxw.generate_text("capital of france is"))
    # for token in wxw.generate_text_stream("1+1"):
    #     print(token, end="")
