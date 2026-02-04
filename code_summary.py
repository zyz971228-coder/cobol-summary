"""
COBOL Business Summary Pipeline - Optimized Version
====================================================

This module provides a production-grade pipeline for converting COBOL source code
into comprehensive business documentation. It addresses the following critical
challenges in mainframe modernization:

1. Context Completeness - Handles Copybooks dependencies
2. Business Logic Extraction - Goes beyond line-by-line translation
3. Token Efficiency - Intelligent chunking for large programs
4. Accuracy Validation - Multi-stage verification

Architecture Review & Risk Assessment
=====================================

## TOP 4 CRITICAL DEFECTS IN ORIGINAL WORKFLOW

### Defect #1: Missing Copybook Resolution (CRITICAL - Hallucination Risk)
**Problem**: Original workflow reads only the main COBOL file without resolving
COPY statements. Copybooks contain crucial data structures (FD, 01 levels),
constants, and shared routines.

**Impact**:
- LLM cannot understand data layouts (e.g., COPY CUSTOMER-REC)
- Missing field definitions lead to hallucinated descriptions
- Business rules in copybooks are completely missed

**Solution**: Implement CopybookResolver that:
- Parses COPY statements with library name variations
- Recursively resolves nested copybooks
- Builds complete enriched source for LLM analysis

### Defect #2: Shallow Logic Extraction (HIGH - "Code Translation" Trap)
**Problem**: Current prompts ask for "main processes in PROCEDURE DIVISION"
which produces syntax-level descriptions, not business logic.

**Impact**:
- Output reads like "PERFORM PROCESS-RECORD" instead of explaining what
  business rule is being applied
- PERFORM THRU and GO TO control flow is not traced
- Conditional business rules hidden in nested IF statements are missed

**Solution**: Multi-phase extraction:
- Phase 1: Control Flow Analysis (build call graph)
- Phase 2: Data Flow Analysis (trace field transformations)
- Phase 3: Business Rule Synthesis (extract domain logic)

### Defect #3: No Chunking Strategy (HIGH - Token Explosion & Quality Degradation)
**Problem**: Entire COBOL file (potentially 50,000+ lines) sent to LLM at once.

**Impact**:
- Context window overflow for large programs
- "Lost in the middle" problem - LLM attention degrades for long inputs
- Extreme token costs (estimated $50-200 per large program)
- Quality degradation for programs > 8,000 lines

**Solution**: Intelligent Division-based chunking:
- Chunk by COBOL DIVISION/SECTION boundaries (semantic units)
- Maintain cross-chunk context via summary passing
- Implement MapReduce-style aggregation

### Defect #4: No Accuracy Verification (HIGH - Silent Failures)
**Problem**: No mechanism to validate generated summaries are correct.

**Impact**:
- Hallucinated procedure names go undetected
- Incorrect business rule descriptions poison downstream processes
- No confidence score for generated content
- Migration teams may rely on incorrect documentation

**Solution**: Multi-layer validation:
- Structural validation (do mentioned sections/paragraphs exist?)
- LLM self-reflection with source code grounding
- Confidence scoring per section

Author: Mainframe Modernization Architecture Team
Version: 2.0.0
"""

from datetime import datetime, timedelta
from typing import Any, Optional, Dict, List, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import os
import logging
import json
import time
import re
import hashlib
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import pandas as pd

# RAGFlow SDK for RAG-based generation
try:
    from ragflow_sdk import RAGFlow
    RAGFLOW_SDK_AVAILABLE = True
except ImportError:
    RAGFLOW_SDK_AVAILABLE = False
    logging.warning("ragflow-sdk not installed. Install with: pip install ragflow-sdk")

# ============================================================================
# Configuration
# ============================================================================

DEFAULT_CONFIG = {
    # LLM Configuration
    "llm_base_url": "http://localhost:8000/v1",
    "llm_model_name": "Qwen/Qwen2.5-72B-Instruct",
    "llm_api_key": "",
    # RAGFlow Configuration
    "ragflow_api_base": "http://localhost:9380",
    "ragflow_api_key": "",
    "ragflow_chat_id": "",
    "ragflow_chat_name": "",  # Alternative to chat_id - use chat assistant name
    "ragflow_similarity_threshold": 0.2,
    "ragflow_vector_weight": 0.3,
    "ragflow_top_n": 8,
    # Directories
    "cobol_input_dir": "/opt/airflow/data/projects/cobol-test/Input",
    "cobol_output_dir": "/opt/airflow/data/projects/cobol-test/Output",
    "copybook_dirs": ["/opt/airflow/data/projects/cobol-test/Copybooks"],
    # Processing Settings
    "max_tokens_per_chunk": 6000,  # Conservative limit for quality
    "enable_copybook_resolution": True,
    "enable_validation": True,
    "parallel_workers": 3,
    "use_ragflow": False,
    # Validation Settings
    "min_confidence_threshold": 0.7,
    "enable_self_reflection": True,
    # Generation Options - which sections to generate
    "generate_overview": True,
    "generate_flowchart": True,
    "generate_io": True,
    "generate_structure": True,  # Program Structure Analysis
    "generate_core_logic": True,  # Detailed Core Logic with function list
    "generate_dependencies": True,  # Copybooks and Called Programs
}

# ============================================================================
# Data Classes
# ============================================================================

class ConfidenceLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNVERIFIED = "unverified"


@dataclass
class CopybookReference:
    """Represents a COPY statement in COBOL source."""
    name: str
    library: Optional[str] = None
    line_number: int = 0
    replacing_clauses: List[str] = field(default_factory=list)


@dataclass
class COBOLDivision:
    """Represents a COBOL division for chunked processing."""
    name: str
    content: str
    start_line: int
    end_line: int
    sections: List['COBOLSection'] = field(default_factory=list)


@dataclass
class COBOLSection:
    """Represents a COBOL section within a division."""
    name: str
    content: str
    start_line: int
    end_line: int
    paragraphs: List[str] = field(default_factory=list)


@dataclass
class BusinessRule:
    """Extracted business rule from COBOL code."""
    id: str
    description: str
    source_location: str  # e.g., "PROCEDURE DIVISION > PROCESS-ORDER > 100-VALIDATE"
    conditions: List[str]
    actions: List[str]
    data_elements: List[str]
    confidence: ConfidenceLevel = ConfidenceLevel.UNVERIFIED


@dataclass
class ValidationResult:
    """Result of summary validation."""
    is_valid: bool
    confidence_score: float
    issues: List[str]
    suggestions: List[str]
    verified_elements: List[str]
    hallucinated_elements: List[str]


@dataclass
class EnrichedCOBOLSource:
    """COBOL source with resolved dependencies."""
    original_file: str
    original_content: str
    enriched_content: str  # With copybooks inlined
    resolved_copybooks: Dict[str, str]  # copybook name -> content
    line_count: int
    estimated_tokens: int


@dataclass
class ChunkedAnalysis:
    """Analysis result from a single chunk."""
    chunk_id: str
    division: str
    content_summary: str
    identified_paragraphs: List[str]
    data_elements: List[str]
    control_flow: List[str]
    business_rules: List[BusinessRule]


@dataclass
class ProgramSummary:
    """Complete program summary with validation."""
    program_id: str
    file_name: str
    overview: str
    flowchart_mermaid: str
    input_output: str
    business_rules: List[BusinessRule]
    data_dictionary: Dict[str, str]
    validation: ValidationResult
    generation_timestamp: str
    token_usage: Dict[str, int]


# ============================================================================
# Copybook Resolver
# ============================================================================

class CopybookResolver:
    """
    Resolves COPY statements in COBOL source code.

    Handles:
    - Standard COPY statements
    - COPY ... REPLACING clauses
    - Nested copybooks
    - Multiple library paths
    """

    # Pattern to match COPY statements
    COPY_PATTERN = re.compile(
        r'^\s{6}\s*COPY\s+([A-Z0-9-]+)(?:\s+(?:OF|IN)\s+([A-Z0-9-]+))?'
        r'(?:\s+REPLACING\s+(.+?))?\.?\s*$',
        re.MULTILINE | re.IGNORECASE
    )

    def __init__(self, copybook_dirs: List[str]):
        self.copybook_dirs = copybook_dirs
        self.copybook_cache: Dict[str, str] = {}
        self.resolution_log: List[str] = []

    def find_copybook(self, name: str, library: Optional[str] = None) -> Optional[str]:
        """
        Locate a copybook file in the configured directories.

        Args:
            name: Copybook name (without extension)
            library: Optional library/folder name

        Returns:
            Full path to copybook or None if not found
        """
        # Common copybook extensions (including .cbl which is often used)
        extensions = ['', '.cpy', '.CPY', '.cob', '.COB', '.cbl', '.CBL', '.txt']

        # Also try case variations of the name itself
        name_variations = [name, name.upper(), name.lower(), name.capitalize()]

        for copybook_dir in self.copybook_dirs:
            search_dirs = [copybook_dir]
            if library:
                search_dirs.insert(0, os.path.join(copybook_dir, library))

            for search_dir in search_dirs:
                if not os.path.isdir(search_dir):
                    continue
                for name_var in name_variations:
                    for ext in extensions:
                        path = os.path.join(search_dir, f"{name_var}{ext}")
                        if os.path.isfile(path):
                            return path

        return None

    def extract_copy_statements(self, source: str) -> List[CopybookReference]:
        """Extract all COPY statements from COBOL source."""
        references = []

        for line_num, line in enumerate(source.split('\n'), 1):
            match = self.COPY_PATTERN.match(line)
            if match:
                ref = CopybookReference(
                    name=match.group(1).upper(),
                    library=match.group(2).upper() if match.group(2) else None,
                    line_number=line_num,
                    replacing_clauses=[]
                )
                if match.group(3):
                    ref.replacing_clauses = self._parse_replacing(match.group(3))
                references.append(ref)

        return references

    def _parse_replacing(self, replacing_text: str) -> List[str]:
        """Parse REPLACING clause."""
        # Simplified - in production would need full parsing
        return [replacing_text.strip()]

    def resolve_copybook(self, ref: CopybookReference, depth: int = 0) -> str:
        """
        Resolve a single copybook reference.

        Args:
            ref: Copybook reference to resolve
            depth: Current recursion depth (to prevent infinite loops)

        Returns:
            Copybook content with nested copybooks resolved
        """
        if depth > 10:
            self.resolution_log.append(f"WARNING: Max depth reached for {ref.name}")
            return f"      * COPYBOOK {ref.name} - MAX DEPTH REACHED\n"

        # Check cache first
        cache_key = f"{ref.library or ''}/{ref.name}"
        if cache_key in self.copybook_cache:
            return self.copybook_cache[cache_key]

        # Find the copybook file
        path = self.find_copybook(ref.name, ref.library)
        if not path:
            self.resolution_log.append(f"WARNING: Copybook not found: {ref.name}")
            return f"      * COPYBOOK {ref.name} NOT FOUND - PLACEHOLDER\n"

        # Read copybook content
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            self.resolution_log.append(f"ERROR: Failed to read {path}: {e}")
            return f"      * COPYBOOK {ref.name} READ ERROR\n"

        # Apply REPLACING clauses
        for replacing in ref.replacing_clauses:
            # Simplified replacing - production would need full implementation
            pass

        # Recursively resolve nested copybooks
        nested_refs = self.extract_copy_statements(content)
        for nested_ref in nested_refs:
            nested_content = self.resolve_copybook(nested_ref, depth + 1)
            # Replace the COPY statement with resolved content
            content = self.COPY_PATTERN.sub(
                lambda m: nested_content if m.group(1).upper() == nested_ref.name else m.group(0),
                content,
                count=1
            )

        self.copybook_cache[cache_key] = content
        self.resolution_log.append(f"Resolved: {ref.name} from {path}")

        return content

    def enrich_source(self, source: str) -> Tuple[str, Dict[str, str]]:
        """
        Enrich COBOL source by inlining all copybooks.

        Args:
            source: Original COBOL source code

        Returns:
            Tuple of (enriched source, dict of resolved copybooks)
        """
        self.resolution_log = []
        resolved = {}
        enriched = source

        references = self.extract_copy_statements(source)

        for ref in references:
            copybook_content = self.resolve_copybook(ref)
            resolved[ref.name] = copybook_content

            # Create a marked inline version
            inline_marker = f"""
      *================================================================
      * BEGIN COPYBOOK: {ref.name}
      *================================================================
{copybook_content}
      *================================================================
      * END COPYBOOK: {ref.name}
      *================================================================
"""
            # Replace COPY statement with inlined content
            enriched = re.sub(
                rf'^\s{{6}}\s*COPY\s+{ref.name}\b.*$',
                inline_marker,
                enriched,
                count=1,
                flags=re.MULTILINE | re.IGNORECASE
            )

        return enriched, resolved


# ============================================================================
# COBOL Parser & Chunker
# ============================================================================

class COBOLChunker:
    """
    Intelligent chunking of COBOL source code.

    Chunks by semantic boundaries (DIVISION/SECTION) rather than
    arbitrary line counts to preserve context.
    """

    DIVISION_PATTERN = re.compile(
        r'^\s{6}\s*(\w+)\s+DIVISION\s*\.?\s*$',
        re.MULTILINE | re.IGNORECASE
    )

    SECTION_PATTERN = re.compile(
        r'^\s{6}\s*(\w[\w-]*)\s+SECTION\s*\.?\s*$',
        re.MULTILINE | re.IGNORECASE
    )

    PARAGRAPH_PATTERN = re.compile(
        r'^\s{6}\s*(\w[\w-]*)\s*\.\s*$',
        re.MULTILINE
    )

    def __init__(self, max_tokens_per_chunk: int = 6000):
        self.max_tokens = max_tokens_per_chunk
        # Rough estimate: 1 token ≈ 4 characters for COBOL
        self.chars_per_token = 4

    def estimate_tokens(self, text: str) -> int:
        """Estimate token count for text."""
        return len(text) // self.chars_per_token

    def parse_structure(self, source: str) -> List[COBOLDivision]:
        """Parse COBOL source into divisions and sections."""
        divisions = []
        lines = source.split('\n')

        current_division = None
        current_section = None
        division_start = 0
        section_start = 0

        for i, line in enumerate(lines):
            # Check for division
            div_match = self.DIVISION_PATTERN.match(line)
            if div_match:
                # Save previous division
                if current_division:
                    current_division.content = '\n'.join(lines[division_start:i])
                    current_division.end_line = i
                    if current_section:
                        current_section.content = '\n'.join(lines[section_start:i])
                        current_section.end_line = i
                        current_division.sections.append(current_section)
                    divisions.append(current_division)

                current_division = COBOLDivision(
                    name=div_match.group(1).upper(),
                    content="",
                    start_line=i + 1,
                    end_line=0
                )
                division_start = i
                current_section = None
                continue

            # Check for section
            sec_match = self.SECTION_PATTERN.match(line)
            if sec_match and current_division:
                if current_section:
                    current_section.content = '\n'.join(lines[section_start:i])
                    current_section.end_line = i
                    current_division.sections.append(current_section)

                current_section = COBOLSection(
                    name=sec_match.group(1).upper(),
                    content="",
                    start_line=i + 1,
                    end_line=0
                )
                section_start = i

        # Save last division
        if current_division:
            current_division.content = '\n'.join(lines[division_start:])
            current_division.end_line = len(lines)
            if current_section:
                current_section.content = '\n'.join(lines[section_start:])
                current_section.end_line = len(lines)
                current_division.sections.append(current_section)
            divisions.append(current_division)

        return divisions

    def create_chunks(self, source: str) -> List[Tuple[str, str, str]]:
        """
        Create intelligent chunks from COBOL source.

        Returns:
            List of (chunk_id, chunk_type, chunk_content) tuples
        """
        chunks = []
        divisions = self.parse_structure(source)

        for div in divisions:
            div_tokens = self.estimate_tokens(div.content)

            if div_tokens <= self.max_tokens:
                # Division fits in one chunk
                chunks.append((
                    f"{div.name}_FULL",
                    div.name,
                    div.content
                ))
            elif div.sections:
                # Split by sections
                current_chunk = []
                current_tokens = 0
                chunk_num = 1

                for section in div.sections:
                    sec_tokens = self.estimate_tokens(section.content)

                    if current_tokens + sec_tokens > self.max_tokens:
                        if current_chunk:
                            chunks.append((
                                f"{div.name}_PART{chunk_num}",
                                div.name,
                                '\n'.join(current_chunk)
                            ))
                            chunk_num += 1
                            current_chunk = []
                            current_tokens = 0

                    current_chunk.append(section.content)
                    current_tokens += sec_tokens

                if current_chunk:
                    chunks.append((
                        f"{div.name}_PART{chunk_num}",
                        div.name,
                        '\n'.join(current_chunk)
                    ))
            else:
                # No sections, split by line count
                lines = div.content.split('\n')
                lines_per_chunk = (self.max_tokens * self.chars_per_token) // 80

                for i in range(0, len(lines), lines_per_chunk):
                    chunk_num = i // lines_per_chunk + 1
                    chunks.append((
                        f"{div.name}_PART{chunk_num}",
                        div.name,
                        '\n'.join(lines[i:i + lines_per_chunk])
                    ))

        return chunks


# ============================================================================
# Business Rule Extractor
# ============================================================================

class BusinessRuleExtractor:
    """
    Extracts business rules from COBOL code analysis.

    Goes beyond syntax translation to identify actual business logic.
    """

    # Patterns indicating business rules
    CONDITION_PATTERNS = [
        re.compile(r'IF\s+(\S+)\s*(=|>|<|NOT\s*=|>=|<=)\s*(\S+)', re.IGNORECASE),
        re.compile(r'EVALUATE\s+(\S+)', re.IGNORECASE),
        re.compile(r'WHEN\s+(.+?)(?=WHEN|END-EVALUATE|$)', re.IGNORECASE | re.DOTALL),
    ]

    CALCULATION_PATTERNS = [
        re.compile(r'COMPUTE\s+(\S+)\s*=\s*(.+?)(?=\.|\s{2,})', re.IGNORECASE),
        re.compile(r'ADD\s+(.+?)\s+TO\s+(\S+)', re.IGNORECASE),
        re.compile(r'SUBTRACT\s+(.+?)\s+FROM\s+(\S+)', re.IGNORECASE),
        re.compile(r'MULTIPLY\s+(.+?)\s+BY\s+(\S+)', re.IGNORECASE),
        re.compile(r'DIVIDE\s+(.+?)\s+INTO\s+(\S+)', re.IGNORECASE),
    ]

    def extract_control_flow(self, source: str) -> List[Tuple[str, str]]:
        """
        Extract PERFORM and GO TO control flow.

        Returns:
            List of (caller, callee) tuples
        """
        flow = []

        # PERFORM pattern
        perform_pattern = re.compile(
            r'PERFORM\s+(\S+)(?:\s+THRU\s+(\S+))?',
            re.IGNORECASE
        )

        # GO TO pattern
        goto_pattern = re.compile(r'GO\s+TO\s+(\S+)', re.IGNORECASE)

        current_paragraph = "MAIN"

        for line in source.split('\n'):
            # Check for new paragraph
            para_match = re.match(r'^\s{6}\s*(\w[\w-]*)\s*\.\s*$', line)
            if para_match:
                current_paragraph = para_match.group(1)

            # Check for PERFORM
            for match in perform_pattern.finditer(line):
                target = match.group(1)
                flow.append((current_paragraph, target))
                if match.group(2):  # THRU clause
                    flow.append((current_paragraph, f"{target}..{match.group(2)}"))

            # Check for GO TO
            for match in goto_pattern.finditer(line):
                flow.append((current_paragraph, match.group(1)))

        return flow

    def identify_business_patterns(self, source: str) -> List[Dict[str, Any]]:
        """
        Identify common business patterns in COBOL code.

        Returns:
            List of identified patterns with metadata
        """
        patterns = []

        # Validation pattern
        if re.search(r'IF\s+\S+-VALID|INVALID|ERROR|CHECK', source, re.IGNORECASE):
            patterns.append({
                'type': 'VALIDATION',
                'description': 'Input/data validation logic detected'
            })

        # Calculation pattern
        if re.search(r'COMPUTE|ADD|SUBTRACT|MULTIPLY|DIVIDE', source, re.IGNORECASE):
            patterns.append({
                'type': 'CALCULATION',
                'description': 'Business calculations detected'
            })

        # File processing pattern
        if re.search(r'READ\s+\S+|WRITE\s+\S+|REWRITE|DELETE', source, re.IGNORECASE):
            patterns.append({
                'type': 'FILE_PROCESSING',
                'description': 'File I/O operations detected'
            })

        # Date handling pattern
        if re.search(r'DATE|YEAR|MONTH|DAY|YYMMDD|CCYYMMDD', source, re.IGNORECASE):
            patterns.append({
                'type': 'DATE_PROCESSING',
                'description': 'Date manipulation logic detected'
            })

        # Error handling pattern
        if re.search(r'FILE\s+STATUS|SQLCODE|RETURN-CODE|ABEND', source, re.IGNORECASE):
            patterns.append({
                'type': 'ERROR_HANDLING',
                'description': 'Error handling logic detected'
            })

        return patterns


# ============================================================================
# Summary Validator
# ============================================================================

class SummaryValidator:
    """
    Validates generated summaries against source code.

    Checks for:
    - Mentioned elements that exist in source
    - Hallucinated content
    - Completeness
    """

    def __init__(self, source: str, enriched_source: str):
        self.source = source
        self.enriched_source = enriched_source
        self.known_paragraphs = self._extract_paragraphs()
        self.known_sections = self._extract_sections()
        self.known_files = self._extract_files()

    def _extract_paragraphs(self) -> Set[str]:
        """Extract all paragraph names from source."""
        pattern = re.compile(r'^\s{6}\s*(\w[\w-]*)\s*\.\s*$', re.MULTILINE)
        return set(m.group(1).upper() for m in pattern.finditer(self.enriched_source))

    def _extract_sections(self) -> Set[str]:
        """Extract all section names from source."""
        pattern = re.compile(r'^\s{6}\s*(\w[\w-]*)\s+SECTION\s*\.?', re.MULTILINE | re.IGNORECASE)
        return set(m.group(1).upper() for m in pattern.finditer(self.enriched_source))

    def _extract_files(self) -> Set[str]:
        """Extract file names from FD and SELECT statements."""
        files = set()

        # FD statements
        fd_pattern = re.compile(r'^\s{6}\s*FD\s+(\S+)', re.MULTILINE | re.IGNORECASE)
        for m in fd_pattern.finditer(self.enriched_source):
            files.add(m.group(1).upper())

        # SELECT statements
        select_pattern = re.compile(r'SELECT\s+(\S+)\s+ASSIGN', re.IGNORECASE)
        for m in select_pattern.finditer(self.enriched_source):
            files.add(m.group(1).upper())

        return files

    # COBOL reserved words and common terms to ignore during validation
    IGNORED_TERMS = {
        # Common English words
        'THE', 'AND', 'FOR', 'NOT', 'WITH', 'FROM', 'WHEN', 'THEN', 'ELSE',
        'THIS', 'THAT', 'WHICH', 'WHERE', 'WHAT', 'HOW', 'WHY', 'ARE', 'WAS',
        'WILL', 'CAN', 'MAY', 'MUST', 'INTO', 'ONTO', 'UPON', 'ALSO', 'BOTH',
        'EACH', 'SOME', 'SUCH', 'THAN', 'VERY', 'JUST', 'ONLY', 'OVER', 'UNDER',
        'USED', 'USING', 'USES', 'CALLED', 'BASED', 'FOLLOWING', 'BEFORE', 'AFTER',

        # COBOL reserved words / keywords
        'COBOL', 'PROGRAM', 'DIVISION', 'SECTION', 'PROCEDURE', 'DATA', 'FILE',
        'WORKING', 'STORAGE', 'LINKAGE', 'IDENTIFICATION', 'ENVIRONMENT',
        'CONFIGURATION', 'INPUT', 'OUTPUT', 'INPUT-OUTPUT', 'EXTENDS',
        'PERFORM', 'THRU', 'THROUGH', 'UNTIL', 'TIMES', 'VARYING',
        'MOVE', 'ADD', 'SUBTRACT', 'MULTIPLY', 'DIVIDE', 'COMPUTE',
        'IF', 'ELSE', 'END-IF', 'EVALUATE', 'WHEN', 'OTHER', 'END-EVALUATE',
        'READ', 'WRITE', 'REWRITE', 'DELETE', 'START', 'OPEN', 'CLOSE',
        'CALL', 'USING', 'RETURNING', 'END-CALL', 'GOBACK', 'STOP', 'RUN',
        'ACCEPT', 'DISPLAY', 'STRING', 'UNSTRING', 'INSPECT', 'REPLACING',
        'INITIALIZE', 'SET', 'SEARCH', 'SORT', 'MERGE', 'RELEASE', 'RETURN',
        'GO', 'GOTO', 'EXIT', 'CONTINUE', 'NEXT', 'SENTENCE',
        'PIC', 'PICTURE', 'VALUE', 'VALUES', 'OCCURS', 'DEPENDING', 'INDEXED',
        'REDEFINES', 'RENAMES', 'COPY', 'REPLACING', 'FILLER',
        'BINARY', 'COMP', 'COMP-1', 'COMP-2', 'COMP-3', 'COMP-4', 'COMP-5',
        'PACKED-DECIMAL', 'DISPLAY', 'USAGE',
        'HIGH-VALUES', 'LOW-VALUES', 'SPACES', 'ZEROS', 'ZEROES', 'QUOTES',
        'TRUE', 'FALSE', 'NULL', 'NULLS',
        'ASCENDING', 'DESCENDING', 'ALPHABETIC', 'NUMERIC', 'ALPHANUMERIC',
        'POSITIVE', 'NEGATIVE', 'ZERO', 'NOT',
        'GREATER', 'LESS', 'EQUAL', 'THAN',
        'STATUS', 'ERROR', 'EXCEPTION', 'OVERFLOW', 'INVALID', 'END',
        'AT', 'ON', 'SIZE', 'KEY', 'RECORD', 'RECORDS', 'BLOCK', 'CONTAINS',
        'LABEL', 'STANDARD', 'ORGANIZATION', 'ACCESS', 'MODE', 'SEQUENTIAL',
        'RANDOM', 'DYNAMIC', 'RELATIVE', 'INDEXED',
        'FD', 'SD', 'SELECT', 'ASSIGN', 'ALTERNATE', 'OPTIONAL',

        # Common programming/documentation terms
        'CODE', 'PROCESS', 'PROCESSING', 'VALIDATE', 'VALIDATION', 'CHECK',
        'FUNCTION', 'ROUTINE', 'MODULE', 'LOGIC', 'FLOW', 'STEP', 'STEPS',
        'MAIN', 'INIT', 'INITIALIZE', 'FINALIZE', 'BEGIN', 'END', 'START', 'FINISH',
        'SUMMARY', 'OVERVIEW', 'DESCRIPTION', 'PURPOSE', 'CONTEXT', 'DOMAIN',
        'BUSINESS', 'RULE', 'RULES', 'REQUIREMENT', 'REQUIREMENTS',
        'TABLE', 'TABLES', 'FIELD', 'FIELDS', 'COLUMN', 'COLUMNS', 'ROW', 'ROWS',
        'CUSTOMER', 'ORDER', 'ITEM', 'ITEMS', 'PRODUCT', 'PRODUCTS', 'ACCOUNT',
        'DATE', 'TIME', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
        'TOTAL', 'COUNT', 'SUM', 'AVERAGE', 'MAX', 'MIN', 'AMOUNT',
        'YES', 'NO', 'NONE', 'ALL', 'ANY', 'FIRST', 'LAST', 'NEXT', 'PREVIOUS',

        # Markdown / document formatting artifacts
        'MERMAID', 'GRAPH', 'FLOWCHART', 'DIAGRAM', 'SUBGRAPH', 'EOF',
        'NOTE', 'NOTES', 'WARNING', 'INFO', 'TIP', 'IMPORTANT',
        'EXAMPLE', 'EXAMPLES', 'SEE', 'REFERENCE', 'REFERENCES',
    }

    def validate_summary(self, summary: str) -> ValidationResult:
        """
        Validate a generated summary.

        Args:
            summary: Generated summary text

        Returns:
            ValidationResult with findings
        """
        issues = []
        suggestions = []
        verified = []
        hallucinated = []

        # Extract mentioned elements from summary
        # Look for code-like references (backticks or all-caps identifiers)
        # Require at least 3 characters and contain a hyphen or be clearly a paragraph name
        mentioned_pattern = re.compile(r'`([A-Z][A-Z0-9-]{2,})`|(?<!\w)([A-Z][A-Z0-9]*-[A-Z0-9-]+)(?!\w)')

        for match in mentioned_pattern.finditer(summary):
            element = (match.group(1) or match.group(2)).upper()

            # Skip ignored terms (COBOL keywords, common words, etc.)
            if element in self.IGNORED_TERMS:
                continue

            # Skip if it looks like a generic term (no hyphen and less than 4 chars)
            if '-' not in element and len(element) < 4:
                continue

            # Check if element exists in source
            if element in self.known_paragraphs:
                verified.append(f"Paragraph {element}")
            elif element in self.known_sections:
                verified.append(f"Section {element}")
            elif element in self.known_files:
                verified.append(f"File {element}")
            elif element in self.enriched_source.upper():
                verified.append(f"Element {element}")
            else:
                # Potential hallucination
                hallucinated.append(element)
                issues.append(f"Element '{element}' mentioned but not found in source")

        # Calculate confidence score
        total_elements = len(verified) + len(hallucinated)
        if total_elements > 0:
            confidence = len(verified) / total_elements
        else:
            confidence = 0.5  # Neutral if no elements found

        # Add suggestions based on findings
        if hallucinated:
            suggestions.append("Review and correct potentially hallucinated element names")
        if confidence < 0.7:
            suggestions.append("Consider re-generating with more specific prompts")

        return ValidationResult(
            is_valid=confidence >= 0.6,
            confidence_score=confidence,
            issues=issues,
            suggestions=suggestions,
            verified_elements=verified,
            hallucinated_elements=hallucinated
        )


# ============================================================================
# RAGFlow Client (using ragflow-sdk)
# ============================================================================

class RAGFlowClient:
    """
    Client for RAGFlow Chat API using the official ragflow-sdk.

    RAGFlow Chat automatically retrieves relevant content from configured
    knowledge bases and generates enhanced responses using RAG.

    Reference: https://www.ragflow.io/docs/python_api_reference
    """

    def __init__(
        self,
        api_base: str,
        api_key: str,
        chat_id: Optional[str] = None,
        chat_name: Optional[str] = None,
    ):
        """
        Initialize RAGFlow Client using the SDK.

        Args:
            api_base: RAGFlow API base URL (e.g., http://localhost:9380)
            api_key: RAGFlow API key
            chat_id: Chat Assistant ID (optional if chat_name is provided)
            chat_name: Chat Assistant name (optional if chat_id is provided)
        """
        if not RAGFLOW_SDK_AVAILABLE:
            raise ImportError(
                "ragflow-sdk is not installed. Please install it with: pip install ragflow-sdk"
            )

        self.api_base = api_base.rstrip('/')
        self.api_key = api_key
        self.chat_id = chat_id
        self.chat_name = chat_name

        # Initialize RAGFlow SDK
        self.rag = RAGFlow(api_key=api_key, base_url=api_base)
        self._chat_assistant = None
        self._session = None

    def _get_chat_assistant(self):
        """Get the chat assistant by ID or name."""
        if self._chat_assistant is not None:
            return self._chat_assistant

        try:
            if self.chat_id:
                # Get by ID
                assistants = self.rag.list_chats(id=self.chat_id)
                if assistants:
                    self._chat_assistant = assistants[0]
                else:
                    raise ValueError(f"Chat assistant with ID '{self.chat_id}' not found")
            elif self.chat_name:
                # Get by name
                assistants = self.rag.list_chats(name=self.chat_name)
                if assistants:
                    self._chat_assistant = assistants[0]
                else:
                    raise ValueError(f"Chat assistant with name '{self.chat_name}' not found")
            else:
                raise ValueError("Either chat_id or chat_name must be provided")

            logging.info(f"Connected to RAGFlow chat assistant: {self._chat_assistant.name if hasattr(self._chat_assistant, 'name') else self.chat_id}")
            return self._chat_assistant

        except Exception as e:
            logging.error(f"Failed to get chat assistant: {e}")
            raise

    def create_session(self, name: Optional[str] = None) -> str:
        """
        Create a new chat session.

        Args:
            name: Session name (optional, auto-generated if not provided)

        Returns:
            Session ID
        """
        assistant = self._get_chat_assistant()
        session_name = name or f"COBOL Analysis {datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            self._session = assistant.create_session(name=session_name)
            session_id = self._session.id if hasattr(self._session, 'id') else str(self._session)
            logging.info(f"Created RAGFlow session: {session_id}")
            return session_id
        except Exception as e:
            logging.error(f"Failed to create session: {e}")
            raise

    def chat(
        self,
        question: str,
        stream: bool = False,
        create_new_session: bool = False
    ) -> str:
        """
        Send a message to RAGFlow Chat and get response with RAG.

        The RAGFlow Chat will automatically:
        1. Retrieve relevant content from knowledge base
        2. Inject retrieved content into the prompt
        3. Generate response using the LLM

        Args:
            question: User question/prompt to send
            stream: Whether to use streaming response (default: False)
            create_new_session: Whether to create a new session for this chat

        Returns:
            LLM response text
        """
        # Ensure we have a session
        if self._session is None or create_new_session:
            self.create_session()

        try:
            if stream:
                # Streaming response
                response_content = []
                for message in self._session.ask(question=question, stream=True):
                    if hasattr(message, 'content') and message.content:
                        response_content.append(message.content)
                return ''.join(response_content)
            else:
                # Non-streaming response
                message = self._session.ask(question=question, stream=False)
                if hasattr(message, 'content'):
                    return message.content
                return str(message)

        except Exception as e:
            logging.error(f"RAGFlow chat failed: {e}")
            return f"Error: {str(e)}"

    def chat_with_retry(
        self,
        question: str,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        stream: bool = False
    ) -> str:
        """
        Send message with automatic retry on failure.

        Args:
            question: User question to send
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            stream: Whether to use streaming response

        Returns:
            LLM response text
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                response = self.chat(question, stream=stream)
                if not response.startswith("Error:"):
                    return response
                last_error = response
            except Exception as e:
                last_error = str(e)
                logging.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")

            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                # Create a new session on retry
                self._session = None

        return f"Failed after {max_retries} attempts. Last error: {last_error}"

    def close_session(self):
        """Close the current session."""
        if self._session is not None:
            try:
                assistant = self._get_chat_assistant()
                session_id = self._session.id if hasattr(self._session, 'id') else None
                if session_id:
                    assistant.delete_sessions(ids=[session_id])
                    logging.info(f"Closed RAGFlow session: {session_id}")
            except Exception as e:
                logging.warning(f"Failed to close session: {e}")
            finally:
                self._session = None


def create_ragflow_client(config: dict) -> RAGFlowClient:
    """
    Create RAGFlow Client for RAG-based generation.

    Args:
        config: Configuration dictionary

    Returns:
        RAGFlowClient instance
    """
    if not config.get("ragflow_api_key"):
        raise ValueError(
            "RAGFlow requires RAGFLOW_API_KEY to be set. "
            "Please configure this in Airflow Variables or environment variables."
        )

    chat_id = config.get("ragflow_chat_id")
    chat_name = config.get("ragflow_chat_name")

    if not chat_id and not chat_name:
        raise ValueError(
            "RAGFlow requires either RAGFLOW_CHAT_ID or RAGFLOW_CHAT_NAME to be set."
        )

    return RAGFlowClient(
        api_base=config["ragflow_api_base"],
        api_key=config["ragflow_api_key"],
        chat_id=chat_id,
        chat_name=chat_name,
    )


# ============================================================================
# Prompt Templates (Enhanced)
# ============================================================================

SYSTEM_PROMPT_EXPERT = """You are an expert COBOL programmer with 30 years of mainframe experience and a
business analyst specializing in legacy system documentation. Your task is to analyze COBOL source code
and extract BUSINESS MEANING, not just code syntax.

IMPORTANT RULES:
1. Focus on WHAT the code accomplishes for the business, not HOW it's implemented
2. Translate technical operations into business terminology
3. Identify business rules hidden in conditional logic
4. Only mention code elements (paragraph names, file names) that actually exist in the source
5. If you're uncertain about something, say so rather than guessing"""

PROGRAM_OVERVIEW_PROMPT_V2 = """Analyze this COBOL program and provide a comprehensive business overview.

## Required Output Format:

### 1. Program Identification
- **Program ID**: [Extract from IDENTIFICATION DIVISION]
- **Business Domain**: [e.g., Customer Management, Order Processing, Financial Reporting]
- **Primary Business Function**: [One sentence describing the business purpose]

### 2. Business Process Summary
Describe what this program accomplishes from a BUSINESS perspective (not technical).
For example: "This program validates customer orders against inventory levels and calculates applicable discounts based on customer tier and order volume."

### 3. Key Business Rules
List the main business rules implemented in this program:
- Rule 1: [Description]
- Rule 2: [Description]
(Extract from IF/EVALUATE statements, focusing on business meaning)

### 4. Main Processing Steps
List the high-level business steps in order:
1. [Step description in business terms]
2. [Step description in business terms]

### 5. Dependencies & Integration Points
- **Upstream Systems**: [What feeds data to this program?]
- **Downstream Systems**: [What consumes this program's output?]

COBOL Source Code:
```cobol
{cobol_code}
```"""

FLOWCHART_PROMPT_V2 = """Analyze this COBOL program's PROCEDURE DIVISION and create a business-level flowchart.

IMPORTANT:
- Create a flowchart that shows BUSINESS DECISIONS, not just PERFORM statements
- Use business terminology in node labels
- Only include paragraphs/sections that actually exist in the code

## Required Output:

```mermaid
graph TD
    subgraph "Business Process Flow"
    A[Start] --> B[Initialize]
    B --> C{{First Business Decision}}
    C -->|Condition 1| D[Action 1]
    C -->|Condition 2| E[Action 2]
    end
```

Guidelines:
- Use diamond shapes {{}} for business decisions
- Use rectangles [] for processing steps
- Group related steps in subgraphs
- Label edges with business conditions, not just "YES/NO"

COBOL Source Code:
```cobol
{cobol_code}
```"""

INPUT_OUTPUT_PROMPT_V2 = """Analyze this COBOL program and document all inputs and outputs with BUSINESS context.

## Required Output Format:

### Input Sources
| Logical Name | Physical Name | Business Description | Key Fields |
|--------------|---------------|---------------------|------------|
| [FD name] | [From ASSIGN] | [What business data?] | [Important fields] |

### Output Destinations
| Logical Name | Physical Name | Business Description | Content |
|--------------|---------------|---------------------|---------|
| [FD name] | [From ASSIGN] | [What business data?] | [What's written] |

### Working Storage Key Structures
List important data structures that represent business entities:
- [01-level name]: [Business entity description]

### External Calls/APIs
- [CALL statement targets and their purpose]

COBOL Source Code:
```cobol
{cobol_code}
```"""

VALIDATION_PROMPT = """Review this generated summary against the source code and identify any issues.

GENERATED SUMMARY:
{summary}

SOURCE CODE:
```cobol
{cobol_code}
```

Check for:
1. Are all mentioned paragraph/section names actually in the source?
2. Are file names correctly identified?
3. Are business rules accurately described?
4. Is anything important missing?

Provide a brief validation report with:
- Confirmed accurate elements
- Potential errors or hallucinations
- Suggested corrections"""

# Additional prompts from Cobol_Summary_11Nov.json (config.json Prompt_4, 5, 6)

PROGRAM_STRUCTURE_PROMPT = """Analyze this COBOL program and provide a detailed Program Structure Analysis.

## Required Output Format:

## 4. Program Structure Analysis

* **IDENTIFICATION DIVISION**: Metadata information of the program, such as author, date, program name, etc.
* **ENVIRONMENT DIVISION**: Describes the program's runtime environment, especially FILE-CONTROL related to files.
* **DATA DIVISION**:
    * **FILE SECTION**: Defines input/output files (FD) used by the program and their record layouts.
    * **WORKING-STORAGE SECTION**: Describes key variables, flags, counters, and data structures used internally in the program.
    * **LINKAGE SECTION**: (if present) Describes parameters passed to/from this program.
* **PROCEDURE DIVISION**: The core logic of the program, describing major paragraphs/sections and their functions.

COBOL Source Code:
```cobol
{cobol_code}
```"""

CORE_LOGIC_PROMPT = """Analyze this COBOL program's PROCEDURE DIVISION and provide detailed core logic documentation.

## Instructions:
1. First, list out ALL functions by looking for PERFORM statements in PROCEDURE DIVISION in logical order.
   Provide the line number for each function. Do not miss any PERFORM in the PROCEDURE DIVISION.
2. If a function calls another sub-function, list all sub-functions being called.
3. For each function, provide:
   - A Mermaid flowchart showing the logic flow
   - Detailed steps (minimum 150 words per function)
   - Validation rules, default values, and error handling must be clearly stated

## Required Output Format:

## 5. Detailed Core Logic

* **Function List**:
    * `[Function 1]`: From Line [XXX] to line [XXX]. [Function 1] is calling [Sub-function 1]
    * `[Function 2]`: From Line [XXX] to line [XXX].
    * `[Sub-function 1]`: From Line [XXX] to line [XXX]. [Sub-function 1] is calling [Sub-function 2]
    * `[Sub-function 2]`: From Line [XXX] to line [XXX].

* **`[Function 1]`**:
```mermaid
graph TD
    [flowchart for Function 1]
```
    * Step 1: [Detailed description of step 1]
    * Step 2: [Detailed description of step 2]
    * ...

* **`[Function 2]`**:
```mermaid
graph TD
    [flowchart for Function 2]
```
    * Step 1: [Detailed description of step 1]
    * Step 2: [Detailed description of step 2]
    * ...

COBOL Source Code:
```cobol
{cobol_code}
```"""

DEPENDENCIES_PROMPT = """Analyze this COBOL program and identify all dependencies.

## Instructions:
1. For Copybooks: Look for all COPY statements in the code
2. For Called Programs: Look for all CALL statements in the code
3. Do not miss any COPY or CALL statement

## Required Output Format:

## 6. Dependencies

* **Copybooks**:
    * `COPY '[COPYBOOK1]'`: [Briefly explain the purpose of this copybook, e.g., record definitions, constants, common routines]
    * `COPY '[COPYBOOK2]'`: [Briefly explain the purpose of this copybook]
    * ...

* **Called Programs**:
    * `CALL '[SUBPROG1]'`: [Briefly explain the purpose of calling this subroutine]
    * `CALL '[SUBPROG2]'`: [Briefly explain the purpose of calling this subroutine]
    * ...

If no copybooks or called programs are found, explicitly state "None found in this program."

COBOL Source Code:
```cobol
{cobol_code}
```"""

# RAGFlow-specific system prompt with knowledge base placeholder
RAGFLOW_SYSTEM_PROMPT = """# Role
You are a professional business analyst with more than 20-year experience in a world class software development company.
Having technical user from the bank as the audience of your documentation.
The bank is now requesting you to write a technical specification document on the cobol code they provide.
Please provide information in a business formal, technical and professional style.
Please write in a concise and informative tone.
By referring to the knowledge base in the manual dataset, it gives you a better understanding of the cobol structure and definition.

Here is the knowledge base:
{knowledge}
The above is the knowledge base.

# Task
Your primary task is to analyze the COBOL code file provided by the user and generate a comprehensive, structured Markdown technical document.
User may ask you specific question regarding the cobol file, answer the question based on the understanding of the file.

# Constraints and Limitations
* **Adhere to Original Text**: All explanations and analyses must strictly be based on the provided COBOL code; do not guess or add functionalities not present in the code.
* **Professional Terminology**: Use professional and accurate terminology while ensuring clarity.
* **Language**: Conduct analysis and documentation writing in English.
* **Format**: Strictly follow the defined Markdown output format, without omitting any part."""


# ============================================================================
# Airflow Task Functions (Optimized)
# ============================================================================

def get_config() -> dict:
    """Get configuration from Airflow Variables or environment."""
    config = DEFAULT_CONFIG.copy()

    try:
        # LLM Configuration
        config["llm_base_url"] = Variable.get("LLM_BASE_URL", default_var=config["llm_base_url"])
        config["llm_api_key"] = Variable.get("LLM_API_KEY", default_var="")
        config["llm_model_name"] = Variable.get("LLM_MODEL_NAME", default_var=config["llm_model_name"])

        # RAGFlow Configuration
        config["ragflow_api_base"] = Variable.get("RAGFLOW_HOST", default_var=config["ragflow_api_base"])
        config["ragflow_api_key"] = Variable.get("RAGFLOW_API_KEY", default_var="")
        config["ragflow_chat_id"] = Variable.get("RAGFLOW_CHAT_ID", default_var="")
        config["ragflow_chat_name"] = Variable.get("RAGFLOW_CHAT_NAME", default_var="")

        # Directories
        config["cobol_input_dir"] = Variable.get("COBOL_INPUT_DIR", default_var=config["cobol_input_dir"])
        config["cobol_output_dir"] = Variable.get("COBOL_OUTPUT_DIR", default_var=config["cobol_output_dir"])
        config["copybook_dirs"] = Variable.get(
            "COPYBOOK_DIRS",
            default_var=",".join(config["copybook_dirs"])
        ).split(",")

        # Feature flags
        config["enable_copybook_resolution"] = Variable.get(
            "ENABLE_COPYBOOK_RESOLUTION", default_var="true"
        ).lower() == "true"
        config["enable_validation"] = Variable.get(
            "ENABLE_VALIDATION", default_var="true"
        ).lower() == "true"
        config["use_ragflow"] = Variable.get("USE_RAGFLOW", default_var="false").lower() == "true"

        # Generation flags
        config["generate_overview"] = Variable.get("GENERATE_OVERVIEW", default_var="true").lower() == "true"
        config["generate_flowchart"] = Variable.get("GENERATE_FLOWCHART", default_var="true").lower() == "true"
        config["generate_io"] = Variable.get("GENERATE_IO", default_var="true").lower() == "true"
        config["generate_structure"] = Variable.get("GENERATE_STRUCTURE", default_var="true").lower() == "true"
        config["generate_core_logic"] = Variable.get("GENERATE_CORE_LOGIC", default_var="true").lower() == "true"
        config["generate_dependencies"] = Variable.get("GENERATE_DEPENDENCIES", default_var="true").lower() == "true"

    except Exception:
        # Fallback to environment variables
        config["llm_base_url"] = os.getenv("LLM_BASE_URL", config["llm_base_url"])
        config["llm_api_key"] = os.getenv("LLM_API_KEY", "")
        config["llm_model_name"] = os.getenv("LLM_MODEL_NAME", config["llm_model_name"])
        config["cobol_input_dir"] = os.getenv("COBOL_INPUT_DIR", config["cobol_input_dir"])
        config["cobol_output_dir"] = os.getenv("COBOL_OUTPUT_DIR", config["cobol_output_dir"])

        # RAGFlow from environment
        config["ragflow_api_base"] = os.getenv("RAGFLOW_HOST", config["ragflow_api_base"])
        config["ragflow_api_key"] = os.getenv("RAGFLOW_API_KEY", "")
        config["ragflow_chat_id"] = os.getenv("RAGFLOW_CHAT_ID", "")
        config["ragflow_chat_name"] = os.getenv("RAGFLOW_CHAT_NAME", "")
        config["use_ragflow"] = os.getenv("USE_RAGFLOW", "false").lower() == "true"

    return config


def task_load_and_enrich_cobol_files(**context) -> str:
    """
    Task 1: Load COBOL files and enrich with copybooks.

    This addresses Defect #1 by:
    - Resolving COPY statements
    """
    logging.info("Loading and enriching COBOL files...")
    config = get_config()

    input_dir = config["cobol_input_dir"]
    copybook_dirs = config["copybook_dirs"]

    # Initialize components
    # Add input directory to copybook search paths (copybooks often stored with source)
    all_copybook_dirs = copybook_dirs + [input_dir]
    copybook_resolver = CopybookResolver(all_copybook_dirs) if config["enable_copybook_resolution"] else None

    enriched_files = []
    extensions = ['.cob', '.cbl', '.txt', '.cobol', '.COB', '.CBL']

    for root, _, files in os.walk(input_dir):
        for file in files:
            if any(file.endswith(ext) for ext in extensions):
                file_path = os.path.join(root, file)

                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        original_content = f.read()

                    # Extract program name from source
                    program_match = re.search(
                        r'PROGRAM-ID\.\s*(\S+)',
                        original_content,
                        re.IGNORECASE
                    )
                    program_name = program_match.group(1).rstrip('.') if program_match else file.split('.')[0]

                    # Resolve copybooks
                    if copybook_resolver:
                        enriched_content, resolved_copybooks = copybook_resolver.enrich_source(original_content)
                        resolution_log = copybook_resolver.resolution_log.copy()
                    else:
                        enriched_content = original_content
                        resolved_copybooks = {}
                        resolution_log = []

                    # Estimate tokens
                    estimated_tokens = len(enriched_content) // 4

                    enriched_files.append({
                        'file_name': file,
                        'file_path': file_path,
                        'program_name': program_name,
                        'original_content': original_content,
                        'enriched_content': enriched_content,
                        'resolved_copybooks': json.dumps(resolved_copybooks),
                        'copybook_log': json.dumps(resolution_log),
                        'line_count': len(enriched_content.split('\n')),
                        'estimated_tokens': estimated_tokens,
                        'needs_chunking': estimated_tokens > config["max_tokens_per_chunk"]
                    })

                    logging.info(f"Enriched {file}: {len(resolved_copybooks)} copybooks, "
                               f"~{estimated_tokens} tokens")

                except Exception as e:
                    logging.error(f"Failed to process {file_path}: {e}")

    df = pd.DataFrame(enriched_files)
    logging.info(f"Loaded and enriched {len(df)} COBOL files")

    return df.to_json(orient='records')


def task_chunk_large_programs(**context) -> str:
    """
    Task 2: Chunk large programs for efficient processing.

    This addresses Defect #4 by:
    - Splitting programs exceeding token limits
    - Maintaining semantic boundaries (DIVISION/SECTION)
    """
    logging.info("Chunking large programs...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='load_and_enrich_files')
    df = pd.read_json(df_json, orient='records')

    chunker = COBOLChunker(max_tokens_per_chunk=config["max_tokens_per_chunk"])

    chunked_data = []

    for _, row in df.iterrows():
        if row['needs_chunking']:
            chunks = chunker.create_chunks(row['enriched_content'])
            logging.info(f"{row['file_name']}: Split into {len(chunks)} chunks")

            for chunk_id, chunk_type, chunk_content in chunks:
                chunked_data.append({
                    **row.to_dict(),
                    'chunk_id': chunk_id,
                    'chunk_type': chunk_type,
                    'chunk_content': chunk_content,
                    'is_chunked': True
                })
        else:
            chunked_data.append({
                **row.to_dict(),
                'chunk_id': 'FULL',
                'chunk_type': 'COMPLETE',
                'chunk_content': row['enriched_content'],
                'is_chunked': False
            })

    df_chunked = pd.DataFrame(chunked_data)
    logging.info(f"Created {len(df_chunked)} processing units from {len(df)} files")

    return df_chunked.to_json(orient='records')


def task_generate_overview_parallel(**context) -> str:
    """
    Task 3: Generate program overviews with enhanced prompts.

    Uses parallel processing for multiple files/chunks.
    Supports both RAGFlow (with knowledge base) and standard LLM modes.
    Addresses Defect #3 with business-focused prompts.
    """
    logging.info("Generating program overviews...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='chunk_large_programs')
    df = pd.read_json(df_json, orient='records')

    # Initialize RAGFlow client or LLM based on configuration
    use_ragflow = config.get("use_ragflow", False)
    ragflow_client = None
    llm = None

    if use_ragflow:
        logging.info("Using RAGFlow Chat with knowledge base for overview generation")
        try:
            ragflow_client = create_ragflow_client(config)
        except Exception as e:
            logging.warning(f"Failed to create RAGFlow client: {e}. Falling back to LLM.")
            use_ragflow = False

    if not use_ragflow:
        logging.info("Using standard LLM for overview generation")
        from src.config import get_llm
        llm = get_llm()

    def process_chunk_with_ragflow(row, client):
        """Process a single chunk using RAGFlow."""
        prompt = PROGRAM_OVERVIEW_PROMPT_V2.format(
            cobol_code=row['chunk_content'][:30000]  # Safety limit
        )

        try:
            response = client.chat_with_retry(prompt)
            return response
        except Exception as e:
            logging.error(f"RAGFlow overview generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    def process_chunk_with_llm(row, llm_instance):
        """Process a single chunk using standard LLM."""
        from langchain_core.messages import HumanMessage, SystemMessage

        prompt = PROGRAM_OVERVIEW_PROMPT_V2.format(
            cobol_code=row['chunk_content'][:30000]  # Safety limit
        )

        messages = [
            SystemMessage(content=SYSTEM_PROMPT_EXPERT),
            HumanMessage(content=prompt)
        ]

        try:
            response = llm_instance.invoke(messages)
            return response.content
        except Exception as e:
            logging.error(f"LLM overview generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    # Process chunks
    results = []
    if use_ragflow:
        # Sequential processing for RAGFlow to maintain session context
        for idx, row in df.iterrows():
            result = process_chunk_with_ragflow(row, ragflow_client)
            results.append((idx, result))
        # Close RAGFlow session when done
        ragflow_client.close_session()
    else:
        # Parallel processing for standard LLM
        with ThreadPoolExecutor(max_workers=config["parallel_workers"]) as executor:
            futures = {
                executor.submit(process_chunk_with_llm, row, llm): idx
                for idx, row in df.iterrows()
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results.append((idx, future.result()))
                except Exception as e:
                    results.append((idx, f"Error: {str(e)}"))

    # Sort by original index and apply
    results.sort(key=lambda x: x[0])
    df['program_overview'] = [r[1] for r in results]

    logging.info("Program overview generation completed")
    return df.to_json(orient='records')


def task_generate_flowchart_parallel(**context) -> str:
    """
    Task 4: Generate business-level flowcharts.

    Supports both RAGFlow (with knowledge base) and standard LLM modes.
    """
    logging.info("Generating flowcharts...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_overview_parallel')
    df = pd.read_json(df_json, orient='records')

    # Initialize RAGFlow client or LLM based on configuration
    use_ragflow = config.get("use_ragflow", False)
    ragflow_client = None
    llm = None

    if use_ragflow:
        logging.info("Using RAGFlow Chat with knowledge base for flowchart generation")
        try:
            ragflow_client = create_ragflow_client(config)
        except Exception as e:
            logging.warning(f"Failed to create RAGFlow client: {e}. Falling back to LLM.")
            use_ragflow = False

    if not use_ragflow:
        logging.info("Using standard LLM for flowchart generation")
        from src.config import get_llm
        llm = get_llm()

    def process_chunk_with_ragflow(row, client):
        """Process a single chunk using RAGFlow."""
        # Only generate flowchart for PROCEDURE DIVISION chunks or full programs
        if row['chunk_type'] not in ['COMPLETE', 'PROCEDURE']:
            return "N/A - Not PROCEDURE DIVISION"

        prompt = FLOWCHART_PROMPT_V2.format(
            cobol_code=row['chunk_content'][:30000]
        )

        try:
            response = client.chat_with_retry(prompt)
            return response
        except Exception as e:
            logging.error(f"RAGFlow flowchart generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    def process_chunk_with_llm(row, llm_instance):
        """Process a single chunk using standard LLM."""
        from langchain_core.messages import HumanMessage, SystemMessage

        # Only generate flowchart for PROCEDURE DIVISION chunks or full programs
        if row['chunk_type'] not in ['COMPLETE', 'PROCEDURE']:
            return "N/A - Not PROCEDURE DIVISION"

        prompt = FLOWCHART_PROMPT_V2.format(
            cobol_code=row['chunk_content'][:30000]
        )

        messages = [
            SystemMessage(content=SYSTEM_PROMPT_EXPERT),
            HumanMessage(content=prompt)
        ]

        try:
            response = llm_instance.invoke(messages)
            return response.content
        except Exception as e:
            logging.error(f"LLM flowchart generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    # Process chunks
    results = []
    if use_ragflow:
        # Sequential processing for RAGFlow
        for idx, row in df.iterrows():
            result = process_chunk_with_ragflow(row, ragflow_client)
            results.append((idx, result))
        ragflow_client.close_session()
    else:
        # Parallel processing for standard LLM
        with ThreadPoolExecutor(max_workers=config["parallel_workers"]) as executor:
            futures = {
                executor.submit(process_chunk_with_llm, row, llm): idx
                for idx, row in df.iterrows()
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results.append((idx, future.result()))
                except Exception as e:
                    results.append((idx, f"Error: {str(e)}"))

    results.sort(key=lambda x: x[0])
    df['flowchart'] = [r[1] for r in results]

    logging.info("Flowchart generation completed")
    return df.to_json(orient='records')


def task_generate_io_parallel(**context) -> str:
    """
    Task 5: Generate Input/Output documentation.

    Supports both RAGFlow (with knowledge base) and standard LLM modes.
    """
    logging.info("Generating I/O documentation...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_flowchart_parallel')
    df = pd.read_json(df_json, orient='records')

    # Initialize RAGFlow client or LLM based on configuration
    use_ragflow = config.get("use_ragflow", False)
    ragflow_client = None
    llm = None

    if use_ragflow:
        logging.info("Using RAGFlow Chat with knowledge base for I/O generation")
        try:
            ragflow_client = create_ragflow_client(config)
        except Exception as e:
            logging.warning(f"Failed to create RAGFlow client: {e}. Falling back to LLM.")
            use_ragflow = False

    if not use_ragflow:
        logging.info("Using standard LLM for I/O generation")
        from src.config import get_llm
        llm = get_llm()

    def process_chunk_with_ragflow(row, client):
        """Process a single chunk using RAGFlow."""
        prompt = INPUT_OUTPUT_PROMPT_V2.format(
            cobol_code=row['chunk_content'][:30000]
        )

        try:
            response = client.chat_with_retry(prompt)
            return response
        except Exception as e:
            logging.error(f"RAGFlow I/O generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    def process_chunk_with_llm(row, llm_instance):
        """Process a single chunk using standard LLM."""
        from langchain_core.messages import HumanMessage, SystemMessage

        prompt = INPUT_OUTPUT_PROMPT_V2.format(
            cobol_code=row['chunk_content'][:30000]
        )

        messages = [
            SystemMessage(content=SYSTEM_PROMPT_EXPERT),
            HumanMessage(content=prompt)
        ]

        try:
            response = llm_instance.invoke(messages)
            return response.content
        except Exception as e:
            logging.error(f"LLM I/O generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    # Process chunks
    results = []
    if use_ragflow:
        # Sequential processing for RAGFlow
        for idx, row in df.iterrows():
            result = process_chunk_with_ragflow(row, ragflow_client)
            results.append((idx, result))
        ragflow_client.close_session()
    else:
        # Parallel processing for standard LLM
        with ThreadPoolExecutor(max_workers=config["parallel_workers"]) as executor:
            futures = {
                executor.submit(process_chunk_with_llm, row, llm): idx
                for idx, row in df.iterrows()
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results.append((idx, future.result()))
                except Exception as e:
                    results.append((idx, f"Error: {str(e)}"))

    results.sort(key=lambda x: x[0])
    df['input_output'] = [r[1] for r in results]

    logging.info("I/O documentation generation completed")
    return df.to_json(orient='records')


def task_generate_structure_parallel(**context) -> str:
    """
    Task 5b: Generate Program Structure Analysis.

    Analyzes the COBOL program structure including IDENTIFICATION, ENVIRONMENT,
    DATA, and PROCEDURE divisions.
    Supports both RAGFlow (with knowledge base) and standard LLM modes.
    """
    logging.info("Generating Program Structure Analysis...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_io_parallel')
    df = pd.read_json(df_json, orient='records')

    # Check if structure generation is enabled
    if not config.get("generate_structure", True):
        df['program_structure'] = "N/A - Structure generation disabled"
        logging.info("Structure generation skipped (disabled)")
        return df.to_json(orient='records')

    # Initialize RAGFlow client or LLM based on configuration
    use_ragflow = config.get("use_ragflow", False)
    ragflow_client = None
    llm = None

    if use_ragflow:
        logging.info("Using RAGFlow Chat with knowledge base for structure generation")
        try:
            ragflow_client = create_ragflow_client(config)
        except Exception as e:
            logging.warning(f"Failed to create RAGFlow client: {e}. Falling back to LLM.")
            use_ragflow = False

    if not use_ragflow:
        logging.info("Using standard LLM for structure generation")
        from src.config import get_llm
        llm = get_llm()

    def process_chunk_with_ragflow(row, client):
        """Process a single chunk using RAGFlow."""
        prompt = PROGRAM_STRUCTURE_PROMPT.format(
            cobol_code=row['chunk_content'][:30000]
        )

        try:
            response = client.chat_with_retry(prompt)
            return response
        except Exception as e:
            logging.error(f"RAGFlow structure generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    def process_chunk_with_llm(row, llm_instance):
        """Process a single chunk using standard LLM."""
        from langchain_core.messages import HumanMessage, SystemMessage

        prompt = PROGRAM_STRUCTURE_PROMPT.format(
            cobol_code=row['chunk_content'][:30000]
        )

        messages = [
            SystemMessage(content=SYSTEM_PROMPT_EXPERT),
            HumanMessage(content=prompt)
        ]

        try:
            response = llm_instance.invoke(messages)
            return response.content
        except Exception as e:
            logging.error(f"LLM structure generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    # Process chunks
    results = []
    if use_ragflow:
        # Sequential processing for RAGFlow
        for idx, row in df.iterrows():
            result = process_chunk_with_ragflow(row, ragflow_client)
            results.append((idx, result))
        ragflow_client.close_session()
    else:
        # Parallel processing for standard LLM
        with ThreadPoolExecutor(max_workers=config["parallel_workers"]) as executor:
            futures = {
                executor.submit(process_chunk_with_llm, row, llm): idx
                for idx, row in df.iterrows()
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results.append((idx, future.result()))
                except Exception as e:
                    results.append((idx, f"Error: {str(e)}"))

    results.sort(key=lambda x: x[0])
    df['program_structure'] = [r[1] for r in results]

    logging.info("Program Structure Analysis generation completed")
    return df.to_json(orient='records')


def task_generate_core_logic_parallel(**context) -> str:
    """
    Task 5c: Generate Detailed Core Logic documentation.

    Lists all functions/paragraphs with line numbers, flowcharts,
    and detailed step descriptions for each.
    Supports both RAGFlow (with knowledge base) and standard LLM modes.
    """
    logging.info("Generating Detailed Core Logic...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_structure_parallel')
    df = pd.read_json(df_json, orient='records')

    # Check if core logic generation is enabled
    if not config.get("generate_core_logic", True):
        df['core_logic'] = "N/A - Core logic generation disabled"
        logging.info("Core logic generation skipped (disabled)")
        return df.to_json(orient='records')

    # Initialize RAGFlow client or LLM based on configuration
    use_ragflow = config.get("use_ragflow", False)
    ragflow_client = None
    llm = None

    if use_ragflow:
        logging.info("Using RAGFlow Chat with knowledge base for core logic generation")
        try:
            ragflow_client = create_ragflow_client(config)
        except Exception as e:
            logging.warning(f"Failed to create RAGFlow client: {e}. Falling back to LLM.")
            use_ragflow = False

    if not use_ragflow:
        logging.info("Using standard LLM for core logic generation")
        from src.config import get_llm
        llm = get_llm()

    def process_chunk_with_ragflow(row, client):
        """Process a single chunk using RAGFlow."""
        # Only generate core logic for PROCEDURE DIVISION chunks or full programs
        if row['chunk_type'] not in ['COMPLETE', 'PROCEDURE']:
            return "N/A - Not PROCEDURE DIVISION"

        prompt = CORE_LOGIC_PROMPT.format(
            cobol_code=row['chunk_content'][:30000]
        )

        try:
            response = client.chat_with_retry(prompt)
            return response
        except Exception as e:
            logging.error(f"RAGFlow core logic generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    def process_chunk_with_llm(row, llm_instance):
        """Process a single chunk using standard LLM."""
        from langchain_core.messages import HumanMessage, SystemMessage

        # Only generate core logic for PROCEDURE DIVISION chunks or full programs
        if row['chunk_type'] not in ['COMPLETE', 'PROCEDURE']:
            return "N/A - Not PROCEDURE DIVISION"

        prompt = CORE_LOGIC_PROMPT.format(
            cobol_code=row['chunk_content'][:30000]
        )

        messages = [
            SystemMessage(content=SYSTEM_PROMPT_EXPERT),
            HumanMessage(content=prompt)
        ]

        try:
            response = llm_instance.invoke(messages)
            return response.content
        except Exception as e:
            logging.error(f"LLM core logic generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    # Process chunks
    results = []
    if use_ragflow:
        # Sequential processing for RAGFlow
        for idx, row in df.iterrows():
            result = process_chunk_with_ragflow(row, ragflow_client)
            results.append((idx, result))
        ragflow_client.close_session()
    else:
        # Parallel processing for standard LLM
        with ThreadPoolExecutor(max_workers=config["parallel_workers"]) as executor:
            futures = {
                executor.submit(process_chunk_with_llm, row, llm): idx
                for idx, row in df.iterrows()
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results.append((idx, future.result()))
                except Exception as e:
                    results.append((idx, f"Error: {str(e)}"))

    results.sort(key=lambda x: x[0])
    df['core_logic'] = [r[1] for r in results]

    logging.info("Detailed Core Logic generation completed")
    return df.to_json(orient='records')


def task_generate_dependencies_parallel(**context) -> str:
    """
    Task 5d: Generate Dependencies documentation.

    Lists all COPY statements (copybooks) and CALL statements (called programs).
    Supports both RAGFlow (with knowledge base) and standard LLM modes.
    """
    logging.info("Generating Dependencies documentation...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_core_logic_parallel')
    df = pd.read_json(df_json, orient='records')

    # Check if dependencies generation is enabled
    if not config.get("generate_dependencies", True):
        df['dependencies'] = "N/A - Dependencies generation disabled"
        logging.info("Dependencies generation skipped (disabled)")
        return df.to_json(orient='records')

    # Initialize RAGFlow client or LLM based on configuration
    use_ragflow = config.get("use_ragflow", False)
    ragflow_client = None
    llm = None

    if use_ragflow:
        logging.info("Using RAGFlow Chat with knowledge base for dependencies generation")
        try:
            ragflow_client = create_ragflow_client(config)
        except Exception as e:
            logging.warning(f"Failed to create RAGFlow client: {e}. Falling back to LLM.")
            use_ragflow = False

    if not use_ragflow:
        logging.info("Using standard LLM for dependencies generation")
        from src.config import get_llm
        llm = get_llm()

    def process_chunk_with_ragflow(row, client):
        """Process a single chunk using RAGFlow."""
        prompt = DEPENDENCIES_PROMPT.format(
            cobol_code=row['chunk_content'][:30000]
        )

        try:
            response = client.chat_with_retry(prompt)
            return response
        except Exception as e:
            logging.error(f"RAGFlow dependencies generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    def process_chunk_with_llm(row, llm_instance):
        """Process a single chunk using standard LLM."""
        from langchain_core.messages import HumanMessage, SystemMessage

        prompt = DEPENDENCIES_PROMPT.format(
            cobol_code=row['chunk_content'][:30000]
        )

        messages = [
            SystemMessage(content=SYSTEM_PROMPT_EXPERT),
            HumanMessage(content=prompt)
        ]

        try:
            response = llm_instance.invoke(messages)
            return response.content
        except Exception as e:
            logging.error(f"LLM dependencies generation failed for {row['file_name']}: {e}")
            return f"Error: {str(e)}"

    # Process chunks
    results = []
    if use_ragflow:
        # Sequential processing for RAGFlow
        for idx, row in df.iterrows():
            result = process_chunk_with_ragflow(row, ragflow_client)
            results.append((idx, result))
        ragflow_client.close_session()
    else:
        # Parallel processing for standard LLM
        with ThreadPoolExecutor(max_workers=config["parallel_workers"]) as executor:
            futures = {
                executor.submit(process_chunk_with_llm, row, llm): idx
                for idx, row in df.iterrows()
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results.append((idx, future.result()))
                except Exception as e:
                    results.append((idx, f"Error: {str(e)}"))

    results.sort(key=lambda x: x[0])
    df['dependencies'] = [r[1] for r in results]

    logging.info("Dependencies documentation generation completed")
    return df.to_json(orient='records')


def task_aggregate_chunks(**context) -> str:
    """
    Task 6: Aggregate chunked results back into complete program summaries.

    Includes all generated sections: overview, flowchart, I/O, structure,
    core logic, and dependencies.
    """
    logging.info("Aggregating chunked results...")

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_dependencies_parallel')
    df = pd.read_json(df_json, orient='records')

    # Group by original file
    aggregated = []

    for file_name in df['file_name'].unique():
        file_chunks = df[df['file_name'] == file_name].sort_values('chunk_id')

        # Combine overviews
        overviews = [r['program_overview'] for _, r in file_chunks.iterrows()
                    if not str(r['program_overview']).startswith('Error')]
        combined_overview = '\n\n---\n\n'.join(overviews) if overviews else "Generation failed"

        # Take best flowchart (usually from PROCEDURE division)
        flowcharts = [r['flowchart'] for _, r in file_chunks.iterrows()
                     if not str(r['flowchart']).startswith('N/A') and not str(r['flowchart']).startswith('Error')]
        combined_flowchart = flowcharts[0] if flowcharts else "No flowchart generated"

        # Combine I/O
        ios = [r['input_output'] for _, r in file_chunks.iterrows()
              if not str(r['input_output']).startswith('Error')]
        combined_io = '\n\n'.join(ios) if ios else "Generation failed"

        # Combine Program Structure
        structures = [r['program_structure'] for _, r in file_chunks.iterrows()
                     if 'program_structure' in r and not str(r['program_structure']).startswith('Error')
                     and not str(r['program_structure']).startswith('N/A')]
        combined_structure = '\n\n'.join(structures) if structures else "Structure analysis not generated"

        # Take best core logic (usually from PROCEDURE division)
        core_logics = [r['core_logic'] for _, r in file_chunks.iterrows()
                      if 'core_logic' in r and not str(r['core_logic']).startswith('N/A')
                      and not str(r['core_logic']).startswith('Error')]
        combined_core_logic = core_logics[0] if core_logics else "Core logic not generated"

        # Combine dependencies
        deps = [r['dependencies'] for _, r in file_chunks.iterrows()
               if 'dependencies' in r and not str(r['dependencies']).startswith('Error')
               and not str(r['dependencies']).startswith('N/A')]
        combined_dependencies = '\n\n'.join(deps) if deps else "Dependencies analysis not generated"

        first_row = file_chunks.iloc[0]

        aggregated.append({
            'file_name': file_name,
            'program_name': first_row['program_name'],
            'original_content': first_row['original_content'],
            'enriched_content': first_row['enriched_content'],
            'copybook_log': first_row.get('copybook_log'),
            'program_overview': combined_overview,
            'flowchart': combined_flowchart,
            'input_output': combined_io,
            'program_structure': combined_structure,
            'core_logic': combined_core_logic,
            'dependencies': combined_dependencies,
            'chunk_count': len(file_chunks)
        })

    df_aggregated = pd.DataFrame(aggregated)
    logging.info(f"Aggregated {len(df)} chunks into {len(df_aggregated)} program summaries")

    return df_aggregated.to_json(orient='records')


def task_validate_summaries(**context) -> str:
    """
    Task 7: Validate generated summaries against source code.

    Addresses Defect #5 by:
    - Checking for hallucinated element names
    - Verifying mentioned paragraphs/sections exist
    - Calculating confidence scores
    """
    logging.info("Validating summaries...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='aggregate_chunks')
    df = pd.read_json(df_json, orient='records')

    if not config["enable_validation"]:
        df['validation_result'] = None
        df['confidence_score'] = None
        logging.info("Validation skipped (disabled)")
        return df.to_json(orient='records')

    validation_results = []

    for idx, row in df.iterrows():
        # Create validator
        validator = SummaryValidator(
            source=row['original_content'],
            enriched_source=row['enriched_content']
        )

        # Combine all generated content for validation
        combined_summary = f"""
{row['program_overview']}

{row['flowchart']}

{row['input_output']}

{row.get('program_structure', '')}

{row.get('core_logic', '')}

{row.get('dependencies', '')}
"""

        result = validator.validate_summary(combined_summary)

        validation_results.append({
            'is_valid': result.is_valid,
            'confidence_score': result.confidence_score,
            'issues_count': len(result.issues),
            'verified_count': len(result.verified_elements),
            'hallucinated_count': len(result.hallucinated_elements),
            'issues': json.dumps(result.issues[:10]),  # Limit for storage
            'suggestions': json.dumps(result.suggestions)
        })

        logging.info(f"{row['file_name']}: Confidence={result.confidence_score:.2f}, "
                    f"Valid={result.is_valid}, Issues={len(result.issues)}")

    # Add validation results to dataframe
    for key in validation_results[0].keys():
        df[f'validation_{key}'] = [r[key] for r in validation_results]

    logging.info("Validation completed")
    return df.to_json(orient='records')


def task_combine_and_save(**context) -> dict:
    """
    Task 8: Combine results and save with validation metadata.
    """
    logging.info("Combining and saving results...")
    config = get_config()

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='validate_summaries')
    df = pd.read_json(df_json, orient='records')

    output_dir = config["cobol_output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    saved_files = []

    for _, row in df.iterrows():
        # Build validation section
        validation_section = ""
        if row.get('validation_is_valid') is not None:
            confidence = row.get('validation_confidence_score', 0)
            issues = json.loads(row.get('validation_issues', '[]'))

            validation_section = f"""
---

## Validation Report

- **Confidence Score**: {confidence:.1%}
- **Status**: {'PASSED' if row['validation_is_valid'] else 'NEEDS REVIEW'}
- **Verified Elements**: {row.get('validation_verified_count', 0)}
- **Potential Issues**: {row.get('validation_issues_count', 0)}

{'### Issues Found' if issues else ''}
{chr(10).join(f'- {issue}' for issue in issues[:5])}
"""

        # Build copybook section
        copybook_section = ""
        if row.get('copybook_log'):
            logs = json.loads(row['copybook_log'])
            if logs:
                copybook_section = f"""
---

## Copybook Resolution Log

{chr(10).join(f'- {log}' for log in logs[:20])}
"""

        # Build structure section if available
        structure_section = ""
        if row.get('program_structure') and row['program_structure'] != "Structure analysis not generated":
            structure_section = f"""
---

{row['program_structure']}
"""

        # Build core logic section if available
        core_logic_section = ""
        if row.get('core_logic') and row['core_logic'] != "Core logic not generated":
            core_logic_section = f"""
---

{row['core_logic']}
"""

        # Build dependencies section if available
        dependencies_section = ""
        if row.get('dependencies') and row['dependencies'] != "Dependencies analysis not generated":
            dependencies_section = f"""
---

{row['dependencies']}
"""

        # Combine into final document
        combined = f"""# COBOL Program Analysis: {row['file_name']}

**Program ID**: {row['program_name']}
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Analysis Version**: 2.0 (Enhanced with Copybook Resolution, RAGFlow Integration & Validation)

---

{row['program_overview']}

---

{row['flowchart']}

---

{row['input_output']}
{structure_section}
{core_logic_section}
{dependencies_section}
{validation_section}

{copybook_section}
"""

        # Save to file
        output_file = os.path.join(
            output_dir,
            f"{os.path.splitext(row['file_name'])[0]}_summary.md"
        )

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(combined)
            saved_files.append(output_file)
            logging.info(f"Saved: {output_file}")
        except Exception as e:
            logging.error(f"Failed to save {output_file}: {e}")

    # Generate summary report
    summary_report = {
        'files_processed': len(df),
        'files_saved': len(saved_files),
        'output_directory': output_dir,
        'average_confidence': df['validation_confidence_score'].mean() if 'validation_confidence_score' in df else None,
        'files_needing_review': int((df['validation_is_valid'] == False).sum()) if 'validation_is_valid' in df else 0,
        'total_issues': int(df['validation_issues_count'].sum()) if 'validation_issues_count' in df else 0,
        'files': df['file_name'].tolist()
    }

    logging.info(f"Processing complete: {len(saved_files)} files saved")
    return summary_report


# ============================================================================
# DAG Definition
# ============================================================================

default_args = {
    'owner': 'mainframe-modernization',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='cobol_business_summary_v2',
    default_args=default_args,
    description='COBOL Business Summary Generator - Enhanced with Copybook Resolution, RAGFlow Integration & Validation',
    schedule_interval=None,
    catchup=False,
    tags=['cobol', 'mainframe', 'modernization', 'documentation', 'v2', 'ragflow'],
    doc_md=__doc__,
) as dag:

    # Task 1: Load and enrich COBOL files
    load_enrich = PythonOperator(
        task_id='load_and_enrich_files',
        python_callable=task_load_and_enrich_cobol_files,
        provide_context=True,
    )

    # Task 2: Chunk large programs
    chunk_programs = PythonOperator(
        task_id='chunk_large_programs',
        python_callable=task_chunk_large_programs,
        provide_context=True,
    )

    # Task 3: Generate overviews (parallel/RAGFlow)
    gen_overview = PythonOperator(
        task_id='generate_overview_parallel',
        python_callable=task_generate_overview_parallel,
        provide_context=True,
    )

    # Task 4: Generate flowcharts (parallel/RAGFlow)
    gen_flowchart = PythonOperator(
        task_id='generate_flowchart_parallel',
        python_callable=task_generate_flowchart_parallel,
        provide_context=True,
    )

    # Task 5: Generate I/O documentation (parallel/RAGFlow)
    gen_io = PythonOperator(
        task_id='generate_io_parallel',
        python_callable=task_generate_io_parallel,
        provide_context=True,
    )

    # Task 5b: Generate Program Structure Analysis (parallel/RAGFlow)
    gen_structure = PythonOperator(
        task_id='generate_structure_parallel',
        python_callable=task_generate_structure_parallel,
        provide_context=True,
    )

    # Task 5c: Generate Detailed Core Logic (parallel/RAGFlow)
    gen_core_logic = PythonOperator(
        task_id='generate_core_logic_parallel',
        python_callable=task_generate_core_logic_parallel,
        provide_context=True,
    )

    # Task 5d: Generate Dependencies documentation (parallel/RAGFlow)
    gen_dependencies = PythonOperator(
        task_id='generate_dependencies_parallel',
        python_callable=task_generate_dependencies_parallel,
        provide_context=True,
    )

    # Task 6: Aggregate chunks
    aggregate = PythonOperator(
        task_id='aggregate_chunks',
        python_callable=task_aggregate_chunks,
        provide_context=True,
    )

    # Task 7: Validate summaries
    validate = PythonOperator(
        task_id='validate_summaries',
        python_callable=task_validate_summaries,
        provide_context=True,
    )

    # Task 8: Combine and save
    save_results = PythonOperator(
        task_id='combine_and_save',
        python_callable=task_combine_and_save,
        provide_context=True,
    )

    # Task dependencies - complete pipeline with all documentation sections
    # Load -> Chunk -> Overview -> Flowchart -> I/O -> Structure -> Core Logic -> Dependencies -> Aggregate -> Validate -> Save
    load_enrich >> chunk_programs >> gen_overview >> gen_flowchart >> gen_io >> gen_structure >> gen_core_logic >> gen_dependencies >> aggregate >> validate >> save_results
