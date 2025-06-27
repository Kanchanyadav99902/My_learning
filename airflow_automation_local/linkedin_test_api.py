
import requests
import pandas as pd
from datetime import datetime, timedelta

# access_token = 'xxx'

# 1 Finance
client_id = '86xux1btqw4fwm'
client_secret = 'KdZftXx0uSFxgWaR'
app_name = '1 Finance Magazine'
# access_token = 'AQXzQAf9ecs1Bc91jzpLVJfjOf58a_JvMdzcqGEGNM6XiQXI829-W8EVNzckUqcKyoTtKbUsbo8vjxboOKZLCEBr3lqXZC4MJw59U1WHP0du1vD00GH9f4rEI-EeFkfSzEn1GfSNvLWIcS5et_j0ya0v7t-jCmUGzqxXApzMcgQXWpiyNjMEYAplgOhC29hauwqhB0aV-VO7KSBvRaE77lkzENgYsYOPW0-w4DxlN_7aW-M5b8WOzkn6Jvm6_uZtVb01LrngiTDdkJX0g9dTZOtCIV4ekhxAl9WnLdemm1l5cLWJdMeZoNY-5Db8c8ChI-dtyEbjnUV7dWqYyF1plDJ7nbgRnA'
access_token = 'AQWOV70ATmYgmmONOpXj44xwwQVt0r1djcIHvRIUdpgZphJOWhesM3teYgHPrGJzAzg-IjR3kATytvYLkKTxuPSyUHSv_KDpUaMdABgyYjB9flvaTFukNowzs05m6URPWvDk_bpy0cjVjtz9HMonC3-FQyMjcDZP3ujIsihmrPK8ndr7yrzYePoEFQb5dOnCRZhwClDo-nuY8xkeNy1c9GF9UxjwzAUD4inPavBD0fHFtRtfqHKyNanvTklwpQd4QgZE1Pb2D9wRksliN7745eb-c0j2bI7pGQ40D87UVHFdXTF0zIR0UGpIIS_0BOjBij8EApodT6FcJOPcves2o2mo-JH-Kw'
refresh_token = 'AQUwVJjJVjKUXKkm2Uo5tlFU9gKgytZ2lfE-bjnfH0z6ux9HHYy7ksH2xcVSVXwgfPu-Xs9ZXLK46Zx_K9WjZ_i3bfT6dSt9lC9Lrw6nSeKoq7XgIBwRTWjqgMmet0TfFRSIqOvyADU1NBd365-9Ydyo0KOkb1MW1IJqaiEx2ME5P46V2_F4cIrQJaIE1cY5N8hvugbOgCqFGz9j4SjSXlLeGzMxd7N0v8kByIB0f-BxClK3i4ev4XS7DNrKHYfyr6RBQRILpFsbpA3XAWynYJrYX7m2UlRlJQ-uJV8hsRgmFArylvubzzPV8BD0v6zC15DxVeSc4ygi4gpS5tfp-oDSFEsU8w'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/j/son',
}
redirect_url = 'https://www.linkedin.com/developers/tools/oauth/redirect'
# endpoint = 'https://www.linkedin.com/company/80708157/admin/analytics/updates/'
endpoint = 'https://api.linkedin.com/v2/adAnalyticsV2'

org_id = "80708157"

org_id = "80708157"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

end_date = datetime.now()
start_date = end_date - timedelta(days=365)
start_epoch = int(start_date.timestamp() * 1000)
end_epoch = int(end_date.timestamp() * 1000)

url = (
    f"https://api.linkedin.com/v2/organizationalEntityShareStatistics"
    f"?q=organizationalEntity"
    f"&organizationalEntity=urn:li:organization:{org_id}"
    f"&timeIntervals.timeGranularityType=DAY"
    f"&timeIntervals.timeRange.start={start_epoch}"
    f"&timeIntervals.timeRange.end={end_epoch}"
)

response = requests.get(url, headers=headers)
data = response.json()

stats_list = []
for element in data.get('elements', []):
    date = element['timeRange']['start']
    date_str = datetime.utcfromtimestamp(date / 1000).strftime('%Y-%m-%d')
    stats = {
        'Date': date_str,
        'Impressions (organic)': element.get('totalShareStatistics', {}).get('impressionCount', 0),
        'Unique impressions (organic)': element.get('totalShareStatistics', {}).get('uniqueImpressionsCount', 0),
        'Clicks (organic)': element.get('totalShareStatistics', {}).get('clickCount', 0),
        'Reactions (organic)': element.get('totalShareStatistics', {}).get('likeCount', 0),
        'Comments (organic)': element.get('totalShareStatistics', {}).get('commentCount', 0),
        'Reposts (organic)': element.get('totalShareStatistics', {}).get('shareCount', 0),
        'Engagement rate (organic)': element.get('totalShareStatistics', {}).get('engagement', 0),
        # Sponsored and totals will be merged later
    }
    stats_list.append(stats)

organic_df = pd.DataFrame(stats_list)
print(organic_df.columns)
print(organic_df)
organic_df.to_csv('linkedin_organic_df.csv', index = False)