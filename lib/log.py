import re
import ast
import csv
import argparse


def process_log(filename):

    regex = (
        "PID \\[\\d*\\] - .{23} - WARNING - "
        "Cannot recover data from Enedis for contract \\[(.*?)\\]: "
        "(.*?)Data sent to Enedis: (.*)"
    )

    issues = []
    with open(filename, "r") as file:
        for line in file:
            matches = re.search(regex, line)
            if matches:
                contrat = matches.group(1)
                error = matches.group(2)
                data = ast.literal_eval(matches.group(3))["demande"]
                issues.append({
                    "contract": contrat,
                    "error": error,
                    "mesuresTypeCode": data["mesuresTypeCode"],
                    "dateDebut": data["dateDebut"],
                    "dateFin": data["dateFin"],
                    "mesuresCorrigees": data["mesuresCorrigees"],
                    "soutirage": data["soutirage"],
                    "injection": data["injection"],
                    "accordClient": data["accordClient"],
                    "grandeurPhysique": data["grandeurPhysique"],
                })

    with open(filename.replace(".log", ".csv"), 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file, fieldnames=issues[0].keys())
        fc.writeheader()
        fc.writerows(issues)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Process errors to csv file'
    )
    parser.add_argument('--log', required=True, help='Log file to process')
    args = parser.parse_args()
    process_log(args.log)
