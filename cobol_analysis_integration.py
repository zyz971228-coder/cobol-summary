"""
COBOL Analysis Integration Module
=================================

This module integrates the enhanced COBOL parser with the Airflow DAG pipeline.
It addresses the following improvements:

1. Injects pre-analyzed structure into LLM prompts
2. Provides thread-safe LLM client handling
3. Implements DATA DIVISION context preservation for PROCEDURE chunks
4. Adds validation against parsed structure (not just regex)

Author: Mainframe Modernization Architecture Team
Version: 3.0.0
"""

import json
import logging
import threading
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from cobol_parser_enhanced import (
    EnhancedCopybookResolver,
    EnhancedCOBOLParser,
    EnhancedBusinessRuleExtractor,
    EnhancedCOBOLChunker,
    COBOLLineParser,
    parse_cobol_file,
)


# ============================================================================
# Thread-Safe LLM Client Wrapper
# ============================================================================

class ThreadSafeLLMClient:
    """
    Thread-safe wrapper for LLM clients.

    Creates separate client instances per thread to avoid race conditions.
    """

    def __init__(self, llm_factory):
        """
        Args:
            llm_factory: Callable that creates a new LLM client instance
        """
        self._llm_factory = llm_factory
        self._local = threading.local()
        self._lock = threading.Lock()

    def get_client(self):
        """Get thread-local LLM client instance."""
        if not hasattr(self._local, 'client'):
            with self._lock:
                if not hasattr(self._local, 'client'):
                    self._local.client = self._llm_factory()
        return self._local.client

    def invoke(self, messages):
        """Thread-safe invoke."""
        client = self.get_client()
        return client.invoke(messages)


# ============================================================================
# Enhanced Prompt Templates
# ============================================================================

ENHANCED_SYSTEM_PROMPT = """# Role
You are a professional business analyst with more than 20-year experience in a world class software development company. Having technical user from the bank as the audience of your documentation. The bank is now requesting you to write a technical specification document on the cobol code they provide. Please provide information in a business formal, technical and professional style. Please write in a concise and informative tone. By referring to the knowledge base in the manual dataset, it gives you a better understanding of the cobol structure and definition.

# Task
Your primary task is to analyze the COBOL code file running on HP non-stop tandem provided by the user and generate a comprehensive, structured Markdown technical document. User may ask you specific question regarding the cobol file, answer the question base on the understanding of the file.

# Constraints and Limitations
* **Adhere to Original Text**: All explanations and analyses must strictly be based on the provided COBOL code; do not guess or add functionalities do not present in the code.
* **Professional Terminology**: Use professional and accurate terminology while ensuring clarity.
* **Language**: Conduct analysis and documentation writing in English.
* **Format**: Strictly follow the defined Markdown output format below, without omitting any part.
* **Line References**: When describing code content, always indicate the source line range (e.g., "From Line X to Line Y") so readers can trace back to the original code.
* **Pre-Analyzed Context**: Use the pre-analyzed structure provided to guide your analysis for accuracy."""


def create_enhanced_overview_prompt(
    cobol_code: str,
    llm_context: str,
    data_context: str = ""
) -> str:
    """Create overview prompt with pre-analyzed context."""
    context_section = ""
    if llm_context:
        context_section = f"""
## Pre-Analyzed Code Structure (Use this to guide your analysis):
{llm_context}

"""

    data_section = ""
    if data_context:
        data_section = f"""
## Data Structures (Reference for understanding procedure logic):
{data_context}

"""

    return f"""{context_section}{data_section}## Your Task:
By analyzing the cobol source code below, provide the Program Overview, write no less than 300 words in this session. Generate the Program Overview one time only, do not repeat. Strictly follow the defined Markdown output format below, without omitting any part. Do not need to show the word count.
For each part of the overview, indicate the source line range in the original code (e.g., "Based on Line X to Line Y").

## 1. Program Overview

* **Program ID**: `[Extracted from IDENTIFICATION DIVISION]` (From Line [X] to Line [Y])
* **Function Description**: A concise summary of the program's main business purpose.
* **Main Processes **: List out all the processes in the cobol program, by looking into the PROCEDURE DIVISION. For each process, indicate its line range (e.g., From Line [X] to Line [Y]).

### COBOL Source Code:
```cobol
{cobol_code[:30000]}
```"""


def create_enhanced_flowchart_prompt(
    cobol_code: str,
    control_flow: List[Dict],
    data_context: str = ""
) -> str:
    """Create flowchart prompt with control flow analysis."""
    flow_section = ""
    if control_flow:
        flow_lines = []
        for entry in control_flow[:30]:  # Limit entries
            if entry['type'] == 'PERFORM':
                flow_lines.append(f"  - {entry['source']} performs {entry['target']}")
            elif entry['type'] == 'PERFORM-THRU':
                flow_lines.append(f"  - {entry['source']} performs {entry['target']} (range)")
            elif entry['type'] == 'GOTO':
                flow_lines.append(f"  - {entry['source']} jumps to {entry['target']}")
            elif entry['type'] == 'CALL':
                flow_lines.append(f"  - {entry['source']} calls external: {entry['target']}")

        if flow_lines:
            flow_section = f"""
## Pre-Analyzed Control Flow (Base your flowchart on this):
{chr(10).join(flow_lines)}

"""

    return f"""{flow_section}## Your Task:
By analyzing the cobol source code below, provide the Flowchart. Use Mermaid syntax to visualize the main execution flow of `PROCEDURE DIVISION`.
Please strictly generate the document in the following Markdown structure:

## 2. Flowchart
(Based on PROCEDURE DIVISION, From Line [X] to Line [Y])

```mermaid
graph TD
    A[Start] --> B[Read Input File];
    B --> C[End of File?];
    C -- Yes --> D[Close Files];
    C -- No --> E[Process Record];
    E --> F[Write to Output File];
    F --> B;
    D --> G[End];
```

### COBOL Source Code:
```cobol
{cobol_code[:30000]}
```"""


def create_enhanced_core_logic_prompt(
    cobol_code: str,
    paragraphs: List[Dict],
    control_flow: List[Dict],
    data_context: str = ""
) -> str:
    """Create core logic prompt with parsed paragraph list."""
    para_section = ""
    if paragraphs:
        para_lines = []
        for p in paragraphs[:50]:  # Limit
            para_lines.append(f"  - `{p['name']}` (lines {p['start_line']}-{p['end_line']})")
        para_section = f"""
## Identified Paragraphs/Functions (from parsing):
{chr(10).join(para_lines)}

"""

    # Build call hierarchy
    call_map: Dict[str, List[str]] = {}
    for entry in control_flow:
        if entry['type'].startswith('PERFORM'):
            if entry['source'] not in call_map:
                call_map[entry['source']] = []
            call_map[entry['source']].append(entry['target'])

    hierarchy_section = ""
    if call_map:
        hierarchy_lines = []
        for source, targets in list(call_map.items())[:20]:
            hierarchy_lines.append(f"  - {source} -> [{', '.join(targets[:5])}]")
        hierarchy_section = f"""
## Call Hierarchy (from parsing):
{chr(10).join(hierarchy_lines)}

"""

    return f"""{para_section}{hierarchy_section}## Your Task:
By analyzing the cobol source code below,
First, list out all the functions by looking for the PERFORM (`perform`) in `PROCEDURE DIVISION` in logical order and provide the line no for that function.
Do not miss out any `perform` in the `PROCEDURE DIVISION`.
If the function is calling another sub-function, please list out all the sub-function being called in the main function.
List all the sub-functions being called as a normal function after all the main function.
Second, provide the mermaid flowchart and detailed steps for each function.
For both flowcharts and steps, it has to be as detailed as possible to capture all detailed logic of the function.
Detailed validation rules, default values, error handling must be clearly stated in the steps.
The steps have to be in sync with the flowchart.
Write no less than 150 words for each function.
Do not need to generate other section such as overview, flowchart, input/output, Program Structure Analysis. Only need to generate the Detailed Core Logic part.
For each function, clearly indicate the source line range (From Line X to Line Y) so readers can trace back to the original code.
provide the Detailed Core Logic of the code in below md format.

## 5. Detailed Core Logic

* **Function List**:
    * `[Function 1]`: From Line [150] to line [200]. [Function 1] is calling [Sub-function 1]
    * `[Function 2]`: From Line [205] to line [249].
	* `[Sub-function 1]`: From Line [833] to line [892]. [Sub-function 1] is calling [Sub-function 2]
	* `[Sub-function 2]`: From Line [916] to line [990].
	...

* **`[Function 1]`** (From Line [150] to Line [200]):
	```mermaid
	graph TD
		flowchart for Function 1
	```
    * [Step 1, describe the first step of Function 1 in detail]
	* [Step 2, describe the second step of Function 1 in detail]
	...

* **`[Function 2]`** (From Line [205] to Line [249]):
	```mermaid
	graph TD
		flowchart for Function 2
	```
    * [Step 1, describe the first step of Function 2 in detail]
	* [Step 2, describe the second step of Function 2 in detail]
	...
...

### COBOL Source Code:
```cobol
{cobol_code[:30000]}
```"""


# ============================================================================
# Enhanced Validation
# ============================================================================

@dataclass
class EnhancedValidationResult:
    """Enhanced validation result with parsed structure comparison."""
    is_valid: bool
    confidence_score: float
    verified_paragraphs: List[str]
    verified_sections: List[str]
    verified_files: List[str]
    hallucinated_elements: List[str]
    missing_important_elements: List[str]
    issues: List[str]
    suggestions: List[str]


class EnhancedSummaryValidator:
    """
    Validates summaries against parsed COBOL structure.

    Improvements over original:
    1. Uses parsed structure instead of regex extraction
    2. Checks for missing important elements
    3. Better hallucination detection with fuzzy matching
    """

    IGNORED_TERMS = {
        'THE', 'AND', 'FOR', 'NOT', 'WITH', 'FROM', 'WHEN', 'THEN', 'ELSE',
        'THIS', 'THAT', 'INTO', 'PERFORM', 'THRU', 'MOVE', 'IF', 'END-IF',
        'COBOL', 'PROGRAM', 'DIVISION', 'SECTION', 'PROCEDURE', 'DATA',
        'FILE', 'WORKING', 'STORAGE', 'READ', 'WRITE', 'OPEN', 'CLOSE',
        'MERMAID', 'GRAPH', 'FLOWCHART', 'SUBGRAPH', 'START', 'END',
    }

    def __init__(
        self,
        parser: EnhancedCOBOLParser,
        original_source: str,
        enriched_source: str
    ):
        self.parser = parser
        self.original_source = original_source
        self.enriched_source = enriched_source

        # Extract known elements from parsed structure
        self.known_paragraphs = set()
        self.known_sections = set()
        self.known_divisions = set()

        for div in parser.divisions:
            self.known_divisions.add(div.name.upper())
            for sec in div.sections:
                self.known_sections.add(sec.name.upper())
                for para in sec.paragraphs:
                    self.known_paragraphs.add(para.name.upper())

        # Extract files from source
        self.known_files = self._extract_files()

        # Extract 01-level items
        self.known_data_items = self._extract_data_items()

    def _extract_files(self) -> set:
        """Extract file names from FD and SELECT statements."""
        files = set()
        import re

        fd_pattern = re.compile(r'^\s*FD\s+([A-Z0-9][\w-]*)', re.MULTILINE | re.IGNORECASE)
        for m in fd_pattern.finditer(self.enriched_source):
            files.add(m.group(1).upper())

        select_pattern = re.compile(r'SELECT\s+([A-Z0-9][\w-]*)\s+ASSIGN', re.IGNORECASE)
        for m in select_pattern.finditer(self.enriched_source):
            files.add(m.group(1).upper())

        return files

    def _extract_data_items(self) -> set:
        """Extract significant data items (01, 77 levels)."""
        items = set()
        import re

        pattern = re.compile(r'^\s*(01|77)\s+([A-Z0-9][\w-]*)', re.MULTILINE | re.IGNORECASE)
        for m in pattern.finditer(self.enriched_source):
            items.add(m.group(2).upper())

        return items

    def validate_summary(self, summary: str) -> EnhancedValidationResult:
        """Validate a generated summary against parsed structure."""
        import re

        issues = []
        suggestions = []
        verified_paras = []
        verified_sections = []
        verified_files = []
        hallucinated = []
        missing_important = []

        # Extract code-like references from summary
        mentioned_pattern = re.compile(
            r'`([A-Z0-9][\w-]+)`|'
            r'(?<![a-z])([A-Z][A-Z0-9]*-[A-Z0-9-]+)(?![a-z])'
        )

        mentioned_elements = set()
        for match in mentioned_pattern.finditer(summary):
            element = (match.group(1) or match.group(2)).upper()
            if element not in self.IGNORED_TERMS and len(element) >= 3:
                mentioned_elements.add(element)

        # Classify each mentioned element
        for element in mentioned_elements:
            if element in self.known_paragraphs:
                verified_paras.append(element)
            elif element in self.known_sections:
                verified_sections.append(element)
            elif element in self.known_files:
                verified_files.append(element)
            elif element in self.known_data_items:
                pass  # Data items are OK
            elif element in self.known_divisions:
                pass  # Division names are OK
            elif self._fuzzy_exists_in_source(element):
                pass  # Found with fuzzy match
            else:
                hallucinated.append(element)
                issues.append(f"Element '{element}' not found in source")

        # Check for missing important elements
        important_paras = self._get_important_paragraphs()
        for para in important_paras:
            if para not in mentioned_elements:
                mentioned_variations = [m for m in mentioned_elements
                                       if para in m or m in para]
                if not mentioned_variations:
                    missing_important.append(para)

        if missing_important:
            suggestions.append(
                f"Consider mentioning these important paragraphs: {', '.join(missing_important[:5])}"
            )

        # Calculate confidence
        total = len(verified_paras) + len(verified_sections) + len(verified_files) + len(hallucinated)
        if total > 0:
            verified_count = len(verified_paras) + len(verified_sections) + len(verified_files)
            confidence = verified_count / total
        else:
            confidence = 0.5  # Neutral if nothing found

        # Penalize for hallucinations
        if hallucinated:
            confidence *= max(0.5, 1 - (len(hallucinated) * 0.1))

        # Add suggestions
        if hallucinated:
            suggestions.append("Review and correct potentially hallucinated element names")
        if confidence < 0.7:
            suggestions.append("Consider re-generating with more specific prompts")

        return EnhancedValidationResult(
            is_valid=confidence >= 0.6 and len(hallucinated) < 5,
            confidence_score=confidence,
            verified_paragraphs=verified_paras,
            verified_sections=verified_sections,
            verified_files=verified_files,
            hallucinated_elements=hallucinated,
            missing_important_elements=missing_important,
            issues=issues,
            suggestions=suggestions
        )

    def _fuzzy_exists_in_source(self, element: str) -> bool:
        """Check if element exists in source with fuzzy matching."""
        # Direct check
        if element in self.enriched_source.upper():
            return True

        # Check with common variations
        variations = [
            element.replace('-', '_'),
            element.replace('_', '-'),
            element.replace('-', ''),
        ]
        for var in variations:
            if var in self.enriched_source.upper():
                return True

        return False

    def _get_important_paragraphs(self) -> List[str]:
        """Identify important paragraphs (entry points, main logic)."""
        important = []

        # First paragraph in PROCEDURE is usually main entry
        proc_div = next((d for d in self.parser.divisions if d.name == 'PROCEDURE'), None)
        if proc_div and proc_div.sections:
            for sec in proc_div.sections:
                if sec.paragraphs:
                    important.append(sec.paragraphs[0].name)
                    break

        # Paragraphs that are called frequently
        call_counts: Dict[str, int] = {}
        for entry in self.parser.get_control_flow():
            target = entry.target.split('..')[0]  # Handle THRU
            call_counts[target] = call_counts.get(target, 0) + 1

        # Top 5 most called
        sorted_calls = sorted(call_counts.items(), key=lambda x: -x[1])
        for para, _ in sorted_calls[:5]:
            if para not in important:
                important.append(para)

        return important


# ============================================================================
# Integration Functions
# ============================================================================

def process_cobol_file_enhanced(
    file_path: str,
    copybook_dirs: List[str],
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a COBOL file with enhanced parsing.

    Returns enriched data for LLM generation tasks.
    """
    # Read source
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        original_source = f.read()

    # Resolve copybooks
    if config.get('enable_copybook_resolution', True) and copybook_dirs:
        resolver = EnhancedCopybookResolver(copybook_dirs)
        enriched_source, resolved_copybooks = resolver.enrich_source(original_source)
        resolution_log = resolver.resolution_log
    else:
        enriched_source = original_source
        resolved_copybooks = {}
        resolution_log = []

    # Parse structure
    parser = EnhancedCOBOLParser(enriched_source)
    divisions = parser.parse()

    # Extract business context
    extractor = EnhancedBusinessRuleExtractor(parser)
    patterns = extractor.extract_patterns()
    llm_context = extractor.generate_context_for_llm()

    # Create chunks with data context
    chunker = EnhancedCOBOLChunker(parser, config.get('max_tokens_per_chunk', 6000))
    chunks = chunker.create_chunks()

    # Get control flow
    control_flow = [
        {
            'source': e.source,
            'target': e.target,
            'type': e.flow_type,
            'line': e.line_number
        }
        for e in parser.get_control_flow()
    ]

    # Get paragraph list
    paragraphs = [
        {
            'name': p.name,
            'start_line': p.start_line,
            'end_line': p.end_line,
            'performs': p.performs,
            'calls': p.calls
        }
        for p in parser.get_all_paragraphs()
    ]

    # Extract program ID
    import re
    program_match = re.search(r'PROGRAM-ID\.\s*(\S+)', original_source, re.IGNORECASE)
    program_name = program_match.group(1).rstrip('.') if program_match else file_path.split('/')[-1].split('.')[0]

    return {
        'file_path': file_path,
        'file_name': file_path.split('/')[-1],
        'program_name': program_name,
        'original_source': original_source,
        'enriched_source': enriched_source,
        'resolved_copybooks': resolved_copybooks,
        'resolution_log': resolution_log,
        'parser': parser,  # For validation
        'llm_context': llm_context,
        'control_flow': control_flow,
        'paragraphs': paragraphs,
        'business_patterns': [
            {
                'type': p.pattern_type,
                'description': p.description,
                'location': p.source_location,
            }
            for p in patterns
        ],
        'chunks': chunks,
        'estimated_tokens': len(enriched_source) // 4,
    }


def generate_overview_with_context(
    processed_data: Dict[str, Any],
    llm_client,  # Can be ThreadSafeLLMClient or RAGFlowClient
    use_ragflow: bool = False
) -> str:
    """Generate overview using enhanced prompts with pre-analyzed context."""
    prompt = create_enhanced_overview_prompt(
        cobol_code=processed_data['enriched_source'],
        llm_context=processed_data['llm_context'],
        data_context=""  # Full source already includes DATA
    )

    if use_ragflow:
        return llm_client.chat_with_retry(prompt)
    else:
        from langchain_core.messages import HumanMessage, SystemMessage
        messages = [
            SystemMessage(content=ENHANCED_SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ]
        response = llm_client.invoke(messages)
        return response.content


def generate_flowchart_with_context(
    processed_data: Dict[str, Any],
    llm_client,
    use_ragflow: bool = False
) -> str:
    """Generate flowchart using pre-analyzed control flow."""
    prompt = create_enhanced_flowchart_prompt(
        cobol_code=processed_data['enriched_source'],
        control_flow=processed_data['control_flow'],
    )

    if use_ragflow:
        return llm_client.chat_with_retry(prompt)
    else:
        from langchain_core.messages import HumanMessage, SystemMessage
        messages = [
            SystemMessage(content=ENHANCED_SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ]
        response = llm_client.invoke(messages)
        return response.content


def generate_core_logic_with_context(
    processed_data: Dict[str, Any],
    llm_client,
    use_ragflow: bool = False
) -> str:
    """Generate core logic using pre-analyzed paragraph list."""
    prompt = create_enhanced_core_logic_prompt(
        cobol_code=processed_data['enriched_source'],
        paragraphs=processed_data['paragraphs'],
        control_flow=processed_data['control_flow'],
    )

    if use_ragflow:
        return llm_client.chat_with_retry(prompt)
    else:
        from langchain_core.messages import HumanMessage, SystemMessage
        messages = [
            SystemMessage(content=ENHANCED_SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ]
        response = llm_client.invoke(messages)
        return response.content


def validate_summary_enhanced(
    processed_data: Dict[str, Any],
    combined_summary: str
) -> EnhancedValidationResult:
    """Validate summary using enhanced validator with parsed structure."""
    validator = EnhancedSummaryValidator(
        parser=processed_data['parser'],
        original_source=processed_data['original_source'],
        enriched_source=processed_data['enriched_source']
    )
    return validator.validate_summary(combined_summary)


# ============================================================================
# Parallel Processing with Thread Safety
# ============================================================================

def process_files_parallel(
    file_paths: List[str],
    copybook_dirs: List[str],
    config: Dict[str, Any],
    llm_factory,
    max_workers: int = 3
) -> List[Dict[str, Any]]:
    """
    Process multiple COBOL files in parallel with thread-safe LLM access.

    Args:
        file_paths: List of COBOL source file paths
        copybook_dirs: Directories to search for copybooks
        config: Configuration dictionary
        llm_factory: Callable that creates new LLM client instances
        max_workers: Number of parallel workers

    Returns:
        List of processed results with generated summaries
    """
    # Create thread-safe LLM client
    safe_llm = ThreadSafeLLMClient(llm_factory)

    results = []

    def process_single_file(file_path: str) -> Dict[str, Any]:
        """Process a single file."""
        try:
            # Parse and enrich
            processed = process_cobol_file_enhanced(file_path, copybook_dirs, config)

            # Generate summaries
            overview = generate_overview_with_context(processed, safe_llm)
            flowchart = generate_flowchart_with_context(processed, safe_llm)
            core_logic = generate_core_logic_with_context(processed, safe_llm)

            processed['overview'] = overview
            processed['flowchart'] = flowchart
            processed['core_logic'] = core_logic

            # Validate
            combined = f"{overview}\n\n{flowchart}\n\n{core_logic}"
            validation = validate_summary_enhanced(processed, combined)

            processed['validation'] = {
                'is_valid': validation.is_valid,
                'confidence_score': validation.confidence_score,
                'verified_count': len(validation.verified_paragraphs) +
                                  len(validation.verified_sections) +
                                  len(validation.verified_files),
                'hallucinated_count': len(validation.hallucinated_elements),
                'issues': validation.issues,
                'suggestions': validation.suggestions
            }

            # Clean up non-serializable objects
            del processed['parser']

            return processed

        except Exception as e:
            logging.error(f"Failed to process {file_path}: {e}")
            return {
                'file_path': file_path,
                'error': str(e)
            }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_file, fp): fp
            for fp in file_paths
        }

        for future in as_completed(futures):
            file_path = futures[future]
            try:
                result = future.result()
                results.append(result)
                logging.info(f"Completed: {file_path}")
            except Exception as e:
                logging.error(f"Exception for {file_path}: {e}")
                results.append({
                    'file_path': file_path,
                    'error': str(e)
                })

    return results


# ============================================================================
# CLI Entry Point
# ============================================================================

if __name__ == '__main__':
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description='Enhanced COBOL Analysis Tool'
    )
    parser.add_argument('input', help='COBOL source file or directory')
    parser.add_argument('--copybooks', '-c', nargs='*', default=[],
                        help='Copybook directories')
    parser.add_argument('--output', '-o', default='./output',
                        help='Output directory')
    parser.add_argument('--parse-only', action='store_true',
                        help='Only parse, do not generate summaries')

    args = parser.parse_args()

    import os

    if os.path.isfile(args.input):
        files = [args.input]
    else:
        extensions = ['.cob', '.cbl', '.cobol', '.COB', '.CBL']
        files = [
            os.path.join(args.input, f)
            for f in os.listdir(args.input)
            if any(f.endswith(ext) for ext in extensions)
        ]

    print(f"Found {len(files)} COBOL file(s)")

    for fp in files:
        result = parse_cobol_file(fp, args.copybooks or [os.path.dirname(fp)])
        print(f"\n=== {fp} ===")
        print(f"Divisions: {[d['name'] for d in result['divisions']]}")
        print(f"Control flow entries: {len(result['control_flow'])}")
        print(f"Business patterns: {len(result['business_patterns'])}")
        print(f"Chunks: {len(result['chunks'])}")

        if result['business_patterns']:
            print("Patterns found:")
            for p in result['business_patterns']:
                print(f"  - {p['type']}: {p['location']}")
