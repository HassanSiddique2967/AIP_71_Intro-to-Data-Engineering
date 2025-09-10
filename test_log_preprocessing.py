import pandas as pd
import re

# Load your raw logs file
df = pd.read_csv("logs_dataset/log-events-viewer-result.csv")

# Regex: supports INFO, TRACE, DEBUG, WARN, ERROR
pattern = re.compile(
    r'^(INFO|TRACE|DEBUG|WARN|ERROR)\s+'   # log level
    r'([\d\-T:,]+)\s+'                     # event time
    r'(\d+)\s+'                            # process id
    r'([\w\.\$]+)\s+'                      # module/class
    r'\[(.*?)\]\s+'                        # thread
    r'(\d+)\s+'                            # line number
    r'(?:\[(.*?)\]\s+)?'                   # optional tag
    r'(.*)$',                              # log text
    re.MULTILINE
)

# Function to extract multiple log entries from one message field
def parse_message(msg):
    results = []
    if not isinstance(msg, str):
        return results
    
    for match in pattern.finditer(msg):
        results.append({
            "event_time": match.group(2),
            "log_level": match.group(1).lower(),
            "process_id": match.group(3),
            "module": match.group(4),
            "thread": match.group(5),
            "line_number": match.group(6),
            "tag": match.group(7),
            "text": match.group(8).strip()
        })
    return results

# Expand parsed logs into new rows
parsed_rows = []
for _, row in df.iterrows():
    entries = parse_message(row["message"])
    for entry in entries:
        parsed_rows.append({
            "timestamp": row["timestamp"],
            **entry,
            "date": entry["event_time"][:10] if entry["event_time"] else None
        })

parsed_df = pd.DataFrame(parsed_rows)

# Save the structured logs
parsed_df.to_csv("logs_dataset/logs_transformed.csv", index=False)

print("Parsing complete. Structured logs written to logs_dataset/logs_transformed.csv")
print(parsed_df.head(15))
