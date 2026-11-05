from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.weather_collector.collector import WeatherCollector


@pytest.fixture
def sample_farm():
    return {
        "farm_id": 1,
        "farm_name": "Tihama Farm - Bayt Al Faqih",
        "latitude": 14.5160,
        "longitude": 43.3240,
    }


@pytest.fixture
def mock_openweather_response():
    return {
        "cod": 200,
        "dt": 1793857701,
        "main": {
            "temp": 29.74,
            "feels_like": 31.46,
            "humidity": 55,
            "pressure": 1014,
        },
        "wind": {
            "speed": 0.71,
        },
        "clouds": {
            "all": 10,
        },
        "rain": {
            "1h": 0.0,
        },
        "weather": [
            {
                "id": 800,
                "main": "Clear",
                "description": "clear sky",
            }
        ],
    }


def test_fetch_farm_weather_payload_contract(sample_farm, mock_openweather_response):
    db_mock = MagicMock()
    producer_mock = MagicMock()

    collector = WeatherCollector(
        db=db_mock,
        producer=producer_mock,
        api_key="test_key",
        base_url="https://mock.weather/api",
    )

    with patch.object(collector.session, "get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_openweather_response
        mock_get.return_value = mock_response

        record = collector.fetch_farm_weather(sample_farm)

        assert record["schema_version"] == 2
        assert record["farm_id"] == 1
        assert record["farm_name"] == "Tihama Farm - Bayt Al Faqih"
        assert record["temperature"] == 29.74
        assert record["humidity"] == 55.0
        assert record["weather_condition"] == "Clear"
        assert record["weather_reading_id"] == "1-1793857701"
        assert "timestamp" in record


def test_poll_and_publish_sends_to_producer(sample_farm, mock_openweather_response):
    db_mock = MagicMock()
    db_mock.load_farms.return_value = [sample_farm]
    producer_mock = MagicMock()

    collector = WeatherCollector(
        db=db_mock,
        producer=producer_mock,
        api_key="test_key",
    )

    with patch.object(collector.session, "get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_openweather_response
        mock_get.return_value = mock_response

        published = collector.poll_and_publish()

        assert published == 1
        producer_mock.send.assert_called_once()
        producer_mock.flush.assert_called_once()
