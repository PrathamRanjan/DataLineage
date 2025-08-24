#!/usr/bin/env python3

import csv
import sys
from pathlib import Path

def generate_mermaid_diagram(csv_file_path):
    """
    Generate a Mermaid diagram from lineage events CSV file
    """
    
    # Read the CSV file
    events = []
    try:
        with open(csv_file_path, 'r') as f:
            reader = csv.DictReader(f)
            events = list(reader)
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
        return ""
    
    if not events:
        print("No events found in CSV file")
        return ""
    
    # Start building the Mermaid diagram
    mermaid_lines = ["graph TD"]
    mermaid_lines.append("")
    
    # Track nodes and relationships
    datasets = set()
    jobs = set()
    relationships = []
    
    # Process each event
    for event in events:
        job_name = event['job_name']
        input_names = event['input_names'].split(',') if event['input_names'] else []
        output_names = event['output_names'].split(',') if event['output_names'] else []
        
        # Add job to jobs set
        if job_name:
            jobs.add(job_name)
        
        # Add datasets to datasets set
        for input_name in input_names:
            if input_name.strip():
                datasets.add(input_name.strip())
                # Add relationship: dataset -> job
                relationships.append((input_name.strip(), job_name, "input"))
        
        for output_name in output_names:
            if output_name.strip():
                datasets.add(output_name.strip())
                # Add relationship: job -> dataset
                relationships.append((job_name, output_name.strip(), "output"))
    
    # Add node definitions
    mermaid_lines.append("    %% Dataset nodes")
    for dataset in sorted(datasets):
        clean_id = dataset.replace(".", "_").replace("-", "_")
        mermaid_lines.append(f"    {clean_id}[(\"{dataset}\")]")
        mermaid_lines.append(f"    style {clean_id} fill:#e1f5fe")
    
    mermaid_lines.append("")
    mermaid_lines.append("    %% Job nodes")
    for job in sorted(jobs):
        if job:
            clean_id = job.replace(".", "_").replace("-", "_")
            mermaid_lines.append(f"    {clean_id}[{{{job}}}]")
            mermaid_lines.append(f"    style {clean_id} fill:#fff3e0")
    
    mermaid_lines.append("")
    mermaid_lines.append("    %% Relationships")
    
    # Add relationships
    added_relationships = set()
    for source, target, rel_type in relationships:
        source_clean = source.replace(".", "_").replace("-", "_")
        target_clean = target.replace(".", "_").replace("-", "_")
        
        # Avoid duplicate relationships
        rel_key = (source_clean, target_clean)
        if rel_key not in added_relationships:
            mermaid_lines.append(f"    {source_clean} --> {target_clean}")
            added_relationships.add(rel_key)
    
    mermaid_lines.append("")
    mermaid_lines.append("    %% Styling")
    mermaid_lines.append("    classDef default fill:#f9f9f9,stroke:#333,stroke-width:2px")
    
    return "\\n".join(mermaid_lines)

def main():
    if len(sys.argv) != 2:
        print("Usage: python make_mermaid.py <path_to_events.csv>")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    
    # Generate the Mermaid diagram
    diagram = generate_mermaid_diagram(csv_file_path)
    
    if diagram:
        print(diagram)
    else:
        print("Failed to generate diagram")
        sys.exit(1)

if __name__ == "__main__":
    main()