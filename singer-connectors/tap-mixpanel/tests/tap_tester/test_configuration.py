config = {
    "test_name": "tap_mixpanel_combined_test",
    "tap_name": "tap-mixpanel",
    "type": "platform.mixpanel",
    "credentials": {
        "api_secret": "TAP_MIXPANEL_API_SECRET"
    },
    "streams" : {
        "annotations": {"date"},
        "cohort_members": {"cohort_id", "distinct_id"},
        "cohorts": {"id"},
        "engage": {"distinct_id"},
        "export": {"event", "time", "distinct_id"},
        "revenue": {"date"}
    },
    "exclude_streams": [
        "annotations",
        "cohort_members",
        "cohorts",
        "engage",
        "funnels"
    ]
}

