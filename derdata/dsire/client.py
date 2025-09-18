# derdata/markets/dsire/client.py
from __future__ import annotations

import os
import json
from typing import Dict, Any, Optional

from dotenv import load_dotenv
import requests

# Load environment variables from .env file
load_dotenv()  # This loads the .env file

# Fetch the API key from the environment variable
api_key = os.getenv("GRIDSTATUS_API_KEY")
if not api_key:
    raise ValueError("API key not found! Please set GRIDSTATUS_API_KEY in your environment or .env file.")

class DSIREClient:
    def __init__(self, api_key: Optional[str] = api_key) -> None:
        self.api_key = api_key
        self.base_url = "https://www.dsireusa.org/api/"

    def get_programs(self, state: Optional[str] = None) -> Dict[str, Any]:
        """Fetch incentive programs for a given state (optional)."""
        url = f"{self.base_url}programs.json?api_key={self.api_key}"
        if state:
            url += f"&state={state}"
        response = requests.get(url)
        return response.json()

    def get_program_details(self, program_id: int) -> Dict[str, Any]:
        """Fetch detailed information for a specific program by ID."""
        url = f"{self.base_url}programs/{program_id}.json?api_key={self.api_key}"
        response = requests.get(url)
        return response.json()

    def get_state_incentives(self) -> Dict[str, Any]:
        """Fetch state incentives summary."""
        url = f"{self.base_url}state_incentives.json?api_key={self.api_key}"
        response = requests.get(url)
        return response.json()