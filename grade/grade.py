#!/usr/bin/env python3
"""Grade Cleric's answers with SREGym's diagnosis checklist.

SREGym's LLM judge (https://github.com/SREGym/SREGym, MIT) answers nine yes/no questions in three dimensions (fault
localization, fault characterization, scope) by comparing an answer against the fault's ground truth, the
`root_cause` in each run's fault.json; an answer passes at a weighted score of 0.70. The judge is Claude Sonnet 4.5
unless GRADER_MODEL names another model.

    export ANTHROPIC_API_KEY=...
    python3 grade/grade.py results/                       # every results/<scenario>/<rep>/{answer.md,fault.json}
    python3 grade/grade.py --answer a.md --fault fault.json

Writes results/grades.json beside the table it prints. Standard library only.
"""

import argparse
import json
import os
import ssl
import sys
import urllib.request
from pathlib import Path


def _checklist() -> dict:
    return json.loads((Path(__file__).resolve().parent / "checklist.json").read_text())


MODEL = os.environ.get("GRADER_MODEL", "claude-sonnet-4-5")
API = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/") + "/v1/messages"

SYSTEM_PROMPT = """\
You are an expert SRE evaluator assessing an AI agent's root cause analysis.

You will be given the ground-truth root cause, the agent's diagnosis, and a checklist of Yes/No
questions grouped into evaluation dimensions. For each dimension: re-read its definition and hints,
gather the diagnosis's claims relevant to it, compare them against the ground truth (the named
target component and resource for localization, concrete root-cause details such as a misconfigured
value, policy or credential for characterization, direct versus secondary impact for scope), then
answer each question.

Rules: answer only from what the diagnosis explicitly states; do not infer anything absent from the
texts; "Yes" always means the correct outcome is present; treat each question independently.

Example (D1): ground truth "The checkout deployment has a misconfigured port"; diagnosis names the
checkout deployment and blames nothing else -> D1-Q1 Yes, D1-Q2 Yes, D1-Q3 Yes.
Example (D2): ground truth "PRODUCT_CATALOG_ADDR points to port 8082 instead of 8080"; diagnosis
names that env var and both port values -> D2-Q1 Yes, D2-Q2 Yes, D2-Q3 Yes.
Example (D3): ground truth "frontend has a missing environment variable"; diagnosis also blames cart
and recommendation, which are victims -> D3-Q1 No, D3-Q2 Yes, D3-Q3 Yes.
"""

ANSWER_TOOL = {
    "name": "submit_checklist",
    "description": "Submit one answer per checklist question.",
    "input_schema": {
        "type": "object",
        "properties": {
            "answers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "The question id exactly as given, e.g. D1-Q2"},
                        "answer": {"type": "string", "enum": ["Yes", "No"]},
                        "evidence": {"type": "string", "description": "At most 30 words quoting the diagnosis"},
                        "confidence": {"type": "string", "enum": ["High", "Medium", "Low"]},
                    },
                    "required": ["id", "answer", "evidence", "confidence"],
                },
            }
        },
        "required": ["answers"],
    },
}


def _prompt(diagnosis: str, root_cause: str) -> str:
    lines = [
        "## Ground-Truth Root Cause",
        root_cause,
        "",
        "## Agent Diagnosis",
        diagnosis,
        "",
        "## Evaluation Checklist",
    ]
    for dimension in _checklist()["dimensions"]:
        lines += [f"\n### {dimension['id']} - {dimension['name']}", dimension["definition"]]
        for question in dimension["questions"]:
            lines.append(f"{question['id']}: {question['text']}")
            if hint := question.get("evaluator_hint"):
                lines.append(f"  Hint: {hint}")
    return "\n".join(lines)


# python.org's macOS builds ship without a CA bundle until "Install Certificates.command" is run; use the system's.
def _ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    if context.cert_store_stats()["x509_ca"] == 0 and not os.environ.get("SSL_CERT_FILE"):
        for bundle in ("/etc/ssl/cert.pem", "/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt"):
            if Path(bundle).exists():
                context.load_verify_locations(cafile=bundle)
                break
    return context


def _ask(diagnosis: str, root_cause: str) -> list[dict]:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("set ANTHROPIC_API_KEY")
    body = {
        "model": MODEL,
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "tools": [ANSWER_TOOL],
        "tool_choice": {"type": "tool", "name": ANSWER_TOOL["name"]},
        "messages": [{"role": "user", "content": _prompt(diagnosis, root_cause)}],
    }
    request = urllib.request.Request(  # noqa: S310 (the Anthropic API, or the endpoint the user set)
        API,
        data=json.dumps(body).encode(),
        headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
    )
    for attempt in range(4):
        try:
            with urllib.request.urlopen(request, timeout=180, context=_ssl_context()) as response:  # noqa: S310
                reply = json.load(response)
            return next(block["input"]["answers"] for block in reply["content"] if block["type"] == "tool_use")
        except Exception:
            if attempt == 3:
                raise
    return []


def score(answers: list[dict]) -> dict:
    checklist = _checklist()
    yes = {a["id"] for a in answers if a["answer"] == "Yes"}
    dimensions = {}
    for dimension in checklist["dimensions"]:
        ids = [q["id"] for q in dimension["questions"]]
        dimensions[dimension["id"]] = round(sum(i in yes for i in ids) / len(ids), 2)
    weights = checklist["scoring"]["weights"]
    composite = round(sum(weights[d] * s for d, s in dimensions.items()), 2)
    return {
        "passed": composite >= checklist["scoring"]["threshold"],
        "composite": composite,
        "dimensions": dimensions,
        "reasoning": "; ".join(f"{a['id']}={a['answer']} ({a['evidence']})" for a in answers),
    }


def grade(answer: Path, fault: Path) -> dict:
    diagnosis = answer.read_text().strip()
    truth = json.loads(fault.read_text())
    if not diagnosis:
        return {"passed": False, "composite": 0.0, "dimensions": {}, "reasoning": "empty answer"}
    if not truth.get("root_cause"):
        sys.exit(f"{fault} has no root_cause: it is a steady world's, not an incident's")
    return score(_ask(diagnosis, truth["root_cause"]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="?", type=Path, help="results/<scenario>/<rep>/{answer.md,fault.json}")
    parser.add_argument("--answer", type=Path)
    parser.add_argument("--fault", type=Path)
    args = parser.parse_args()
    if args.answer and args.fault:
        print(json.dumps(grade(args.answer, args.fault), indent=2))
        return
    if not args.results:
        parser.error("give a results directory, or --answer and --fault")
    grades: dict[str, dict[str, dict]] = {}
    for answer in sorted(args.results.glob("*/*/answer.md")):
        rep, scenario = answer.parent.name, answer.parent.parent.name
        # One answer the grader can't reach doesn't lose the others: it is recorded and left out of the counts.
        try:
            grades.setdefault(scenario, {})[rep] = grade(answer, answer.parent / "fault.json")
        except Exception as e:
            grades.setdefault(scenario, {})[rep] = {"error": f"{type(e).__name__}: {e}"}
            print(f"{scenario}/{rep}: not graded: {type(e).__name__}: {e}", file=sys.stderr)
    (args.results / "grades.json").write_text(json.dumps(grades, indent=2))
    print(f"{'scenario':26} {'passed':>8} {'mean score':>11} {'not graded':>11}")
    for scenario, reps in grades.items():
        graded = [g for g in reps.values() if "error" not in g]
        passed = sum(g["passed"] for g in graded)
        mean = sum(g["composite"] for g in graded) / len(graded) if graded else 0.0
        print(f"{scenario:26} {passed:>3}/{len(graded):<4} {mean:>11.2f} {len(reps) - len(graded):>11}")


if __name__ == "__main__":
    main()
