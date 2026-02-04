"""
Enhanced COBOL Parser Module - Addressing Critical Defects
==========================================================

This module provides production-grade COBOL parsing with proper handling of:
1. Area A/Area B column sensitivity
2. Comment and continuation line processing
3. Complete COPY REPLACING implementation
4. Accurate paragraph/section identification within PROCEDURE DIVISION
5. Complete PERFORM variant handling (THRU, UNTIL, VARYING, TIMES)
6. CALL statement extraction

Author: Mainframe Modernization Architecture Team
Version: 3.0.0 - Enhanced Parser
"""

import re
import os
import logging
from typing import List, Dict, Tuple, Optional, Set, Any
from dataclasses import dataclass, field
from enum import Enum


# ============================================================================
# Constants for COBOL Column Layout
# ============================================================================

class COBOLColumn:
    """COBOL fixed-format column positions (1-indexed)."""
    SEQUENCE_START = 1
    SEQUENCE_END = 6
    INDICATOR = 7
    AREA_A_START = 8
    AREA_A_END = 11
    AREA_B_START = 12
    AREA_B_END = 72
    IDENTIFICATION_START = 73
    IDENTIFICATION_END = 80


class IndicatorType(Enum):
    """COBOL column 7 indicator types."""
    NORMAL = ' '
    COMMENT = '*'
    COMMENT_SLASH = '/'
    DEBUG = 'D'
    DEBUG_LOWER = 'd'
    CONTINUATION = '-'


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class COBOLLine:
    """Represents a parsed COBOL line with column-aware processing."""
    line_number: int
    raw_text: str
    sequence_area: str = ""      # Columns 1-6
    indicator: str = " "         # Column 7
    area_a: str = ""             # Columns 8-11
    area_b: str = ""             # Columns 12-72
    content: str = ""            # Columns 8-72 combined
    is_comment: bool = False
    is_continuation: bool = False
    is_debug: bool = False
    is_blank: bool = False


@dataclass
class CopybookReference:
    """Represents a COPY statement in COBOL source."""
    name: str
    library: Optional[str] = None
    line_number: int = 0
    replacing_clauses: List[Tuple[str, str]] = field(default_factory=list)
    suppress: bool = False


@dataclass
class COBOLDivision:
    """Represents a COBOL division with enhanced metadata."""
    name: str
    content: str
    start_line: int
    end_line: int
    sections: List['COBOLSection'] = field(default_factory=list)
    raw_lines: List[COBOLLine] = field(default_factory=list)


@dataclass
class COBOLSection:
    """Represents a COBOL section within a division."""
    name: str
    content: str
    start_line: int
    end_line: int
    paragraphs: List['COBOLParagraph'] = field(default_factory=list)


@dataclass
class COBOLParagraph:
    """Represents a COBOL paragraph with detailed info."""
    name: str
    start_line: int
    end_line: int
    content: str
    performs: List[str] = field(default_factory=list)
    gotos: List[str] = field(default_factory=list)
    calls: List[str] = field(default_factory=list)


@dataclass
class ControlFlowEntry:
    """Represents a control flow relationship."""
    source: str           # Caller paragraph/section
    target: str           # Called paragraph/section/program
    flow_type: str        # PERFORM, PERFORM-THRU, PERFORM-UNTIL, GOTO, CALL
    line_number: int
    condition: Optional[str] = None  # For PERFORM UNTIL/VARYING


@dataclass
class BusinessPattern:
    """Identified business pattern with context."""
    pattern_type: str
    description: str
    source_location: str
    evidence: List[str]  # Code snippets showing the pattern
    confidence: float = 1.0


# ============================================================================
# Enhanced COBOL Line Parser
# ============================================================================

class COBOLLineParser:
    """
    Parses COBOL source with proper column sensitivity.

    Handles:
    - Fixed format (columns 1-80)
    - Free format detection
    - Comment lines (*, /)
    - Debug lines (D, d)
    - Continuation lines (-)
    """

    def __init__(self, source: str):
        self.source = source
        self.lines: List[COBOLLine] = []
        self.is_free_format = self._detect_format()

    def _detect_format(self) -> bool:
        """Detect if source is free format or fixed format."""
        # Check for common free format indicators
        first_lines = self.source.split('\n')[:20]
        for line in first_lines:
            if line.strip().upper().startswith('>>SOURCE FORMAT FREE'):
                return True
            # If we see content starting before column 7, likely fixed format
            if len(line) >= 7 and line[6:7] not in ' */-Dd' and line[:6].strip().isdigit():
                return False
        return False

    def parse(self) -> List[COBOLLine]:
        """Parse all lines with column awareness."""
        self.lines = []
        raw_lines = self.source.split('\n')

        for i, raw in enumerate(raw_lines, 1):
            if self.is_free_format:
                parsed = self._parse_free_format_line(i, raw)
            else:
                parsed = self._parse_fixed_format_line(i, raw)
            self.lines.append(parsed)

        # Process continuation lines
        self._process_continuations()

        return self.lines

    def _parse_fixed_format_line(self, line_num: int, raw: str) -> COBOLLine:
        """Parse a fixed-format COBOL line."""
        # Pad line to 80 characters if shorter
        padded = raw.ljust(80) if len(raw) < 80 else raw

        line = COBOLLine(
            line_number=line_num,
            raw_text=raw,
            sequence_area=padded[0:6],
            indicator=padded[6:7] if len(padded) > 6 else ' ',
            area_a=padded[7:11] if len(padded) > 7 else '',
            area_b=padded[11:72] if len(padded) > 11 else '',
        )

        line.content = (line.area_a + line.area_b).rstrip()
        line.is_comment = line.indicator in ('*', '/')
        line.is_continuation = line.indicator == '-'
        line.is_debug = line.indicator in ('D', 'd')
        line.is_blank = not line.content.strip()

        return line

    def _parse_free_format_line(self, line_num: int, raw: str) -> COBOLLine:
        """Parse a free-format COBOL line."""
        stripped = raw.strip()

        line = COBOLLine(
            line_number=line_num,
            raw_text=raw,
            content=stripped,
            is_comment=stripped.startswith('*>') or stripped.startswith('*'),
            is_blank=not stripped,
        )

        return line

    def _process_continuations(self):
        """Join continuation lines to their predecessor."""
        i = 0
        while i < len(self.lines):
            if self.lines[i].is_continuation and i > 0:
                # Find the previous non-blank, non-comment line
                prev_idx = i - 1
                while prev_idx >= 0 and (self.lines[prev_idx].is_comment or
                                          self.lines[prev_idx].is_blank):
                    prev_idx -= 1

                if prev_idx >= 0:
                    # Append continuation content (strip leading spaces from Area B)
                    continuation_text = self.lines[i].area_b.lstrip()
                    self.lines[prev_idx].content += continuation_text
                    self.lines[i].content = ""  # Clear the continuation line
            i += 1

    def get_code_lines(self) -> List[COBOLLine]:
        """Return only executable code lines (no comments, debug, blanks)."""
        return [line for line in self.lines
                if not line.is_comment and not line.is_debug and not line.is_blank]

    def get_content_as_string(self, include_comments: bool = False) -> str:
        """Reconstruct source from parsed lines."""
        if include_comments:
            return '\n'.join(line.content for line in self.lines)
        return '\n'.join(line.content for line in self.get_code_lines())


# ============================================================================
# Enhanced Copybook Resolver
# ============================================================================

class EnhancedCopybookResolver:
    """
    Resolves COPY statements with full REPLACING support.

    Improvements over original:
    1. Column-aware COPY detection
    2. Full REPLACING clause parsing and execution
    3. Better handling of multi-line COPY statements
    4. Library path resolution with OS variations
    """

    # Pattern for COPY statement (works on processed content)
    COPY_PATTERN = re.compile(
        r'\bCOPY\s+([A-Z0-9][\w-]*)'
        r'(?:\s+(?:OF|IN)\s+([A-Z0-9][\w-]*))?'
        r'(?:\s+SUPPRESS\b)?'
        r'(?:\s+REPLACING\s+(.+?))?'
        r'\s*\.',
        re.IGNORECASE | re.DOTALL
    )

    # Pattern for REPLACING clause items
    REPLACING_ITEM_PATTERN = re.compile(
        r'(==.+?==|[\w-]+)\s+BY\s+(==.+?==|[\w-]+)',
        re.IGNORECASE
    )

    def __init__(self, copybook_dirs: List[str]):
        self.copybook_dirs = [d for d in copybook_dirs if os.path.isdir(d)]
        self.copybook_cache: Dict[str, str] = {}
        self.resolution_log: List[str] = []
        self.failed_resolutions: List[str] = []

    def find_copybook(self, name: str, library: Optional[str] = None) -> Optional[str]:
        """Locate copybook file with enhanced search."""
        extensions = ['', '.cpy', '.CPY', '.cob', '.COB', '.cbl', '.CBL',
                      '.copy', '.COPY', '.txt', '.TXT']
        name_variations = [name, name.upper(), name.lower(),
                          name.replace('-', '_'), name.replace('_', '-')]

        search_dirs = []
        for base_dir in self.copybook_dirs:
            if library:
                # Try library subdirectory first
                search_dirs.append(os.path.join(base_dir, library))
                search_dirs.append(os.path.join(base_dir, library.upper()))
                search_dirs.append(os.path.join(base_dir, library.lower()))
            search_dirs.append(base_dir)

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
        """Extract all COPY statements from source."""
        references = []

        # First, use line parser to get clean content
        parser = COBOLLineParser(source)
        parser.parse()
        clean_source = parser.get_content_as_string(include_comments=False)

        for match in self.COPY_PATTERN.finditer(clean_source):
            name = match.group(1).upper()
            library = match.group(2).upper() if match.group(2) else None
            replacing_text = match.group(3)
            suppress = 'SUPPRESS' in match.group(0).upper()

            replacing_clauses = []
            if replacing_text:
                replacing_clauses = self._parse_replacing(replacing_text)

            # Find line number in original source
            line_num = source[:match.start()].count('\n') + 1

            references.append(CopybookReference(
                name=name,
                library=library,
                line_number=line_num,
                replacing_clauses=replacing_clauses,
                suppress=suppress
            ))

        return references

    def _parse_replacing(self, replacing_text: str) -> List[Tuple[str, str]]:
        """Parse REPLACING clause into (old, new) tuples."""
        replacements = []

        for match in self.REPLACING_ITEM_PATTERN.finditer(replacing_text):
            old_text = match.group(1)
            new_text = match.group(2)

            # Remove == delimiters if present
            if old_text.startswith('==') and old_text.endswith('=='):
                old_text = old_text[2:-2]
            if new_text.startswith('==') and new_text.endswith('=='):
                new_text = new_text[2:-2]

            replacements.append((old_text.strip(), new_text.strip()))

        return replacements

    def _apply_replacing(self, content: str, replacements: List[Tuple[str, str]]) -> str:
        """Apply REPLACING substitutions to copybook content."""
        result = content
        for old_text, new_text in replacements:
            # Word boundary replacement to avoid partial matches
            pattern = re.compile(r'\b' + re.escape(old_text) + r'\b', re.IGNORECASE)
            result = pattern.sub(new_text, result)
        return result

    def resolve_copybook(self, ref: CopybookReference, depth: int = 0) -> str:
        """Resolve a copybook with full REPLACING support."""
        if depth > 10:
            self.resolution_log.append(f"WARNING: Max depth reached for {ref.name}")
            return f"      * COPYBOOK {ref.name} - MAX DEPTH REACHED\n"

        cache_key = f"{ref.library or ''}/{ref.name}/{hash(tuple(ref.replacing_clauses))}"
        if cache_key in self.copybook_cache:
            return self.copybook_cache[cache_key]

        path = self.find_copybook(ref.name, ref.library)
        if not path:
            self.resolution_log.append(f"WARNING: Copybook not found: {ref.name}")
            self.failed_resolutions.append(ref.name)
            return f"      * COPYBOOK {ref.name} NOT FOUND - PLACEHOLDER\n"

        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            self.resolution_log.append(f"ERROR: Failed to read {path}: {e}")
            return f"      * COPYBOOK {ref.name} READ ERROR\n"

        # Apply REPLACING clauses
        if ref.replacing_clauses:
            content = self._apply_replacing(content, ref.replacing_clauses)
            self.resolution_log.append(
                f"Applied REPLACING to {ref.name}: {len(ref.replacing_clauses)} substitutions"
            )

        # Recursively resolve nested copybooks
        nested_refs = self.extract_copy_statements(content)
        for nested_ref in nested_refs:
            nested_content = self.resolve_copybook(nested_ref, depth + 1)
            # Replace the specific COPY statement
            content = self._replace_copy_statement(content, nested_ref, nested_content)

        self.copybook_cache[cache_key] = content
        self.resolution_log.append(f"Resolved: {ref.name} from {path}")

        return content

    def _replace_copy_statement(self, source: str, ref: CopybookReference,
                                 replacement: str) -> str:
        """Replace a specific COPY statement with resolved content."""
        # Build pattern for this specific COPY
        library_part = rf'(?:\s+(?:OF|IN)\s+{ref.library})?' if ref.library else r'(?:\s+(?:OF|IN)\s+\w+)?'
        pattern = re.compile(
            rf'\bCOPY\s+{ref.name}{library_part}(?:\s+SUPPRESS)?(?:\s+REPLACING\s+.+?)?\s*\.',
            re.IGNORECASE | re.DOTALL
        )

        marker = f"""
      *================================================================
      * BEGIN COPYBOOK: {ref.name}
      *================================================================
{replacement}
      *================================================================
      * END COPYBOOK: {ref.name}
      *================================================================
"""
        return pattern.sub(marker, source, count=1)

    def enrich_source(self, source: str) -> Tuple[str, Dict[str, str]]:
        """Enrich COBOL source by inlining all copybooks."""
        self.resolution_log = []
        self.failed_resolutions = []
        resolved = {}
        enriched = source

        references = self.extract_copy_statements(source)

        for ref in references:
            copybook_content = self.resolve_copybook(ref)
            resolved[ref.name] = copybook_content
            enriched = self._replace_copy_statement(enriched, ref, copybook_content)

        return enriched, resolved


# ============================================================================
# Enhanced COBOL Structure Parser
# ============================================================================

class EnhancedCOBOLParser:
    """
    Parses COBOL structure with accurate division/section/paragraph identification.

    Improvements:
    1. Area A detection for divisions/sections/paragraphs
    2. Tracks PROCEDURE DIVISION boundary for paragraph identification
    3. Extracts complete control flow including all PERFORM variants and CALL
    """

    # Patterns that must start in Area A (columns 8-11)
    DIVISION_KEYWORDS = ['IDENTIFICATION', 'ENVIRONMENT', 'DATA', 'PROCEDURE']
    SECTION_KEYWORDS_DATA = ['FILE', 'WORKING-STORAGE', 'LOCAL-STORAGE',
                             'LINKAGE', 'COMMUNICATION', 'REPORT', 'SCREEN']

    def __init__(self, source: str):
        self.source = source
        self.line_parser = COBOLLineParser(source)
        self.lines = self.line_parser.parse()
        self.divisions: List[COBOLDivision] = []
        self.procedure_start_line: int = 0
        self.procedure_end_line: int = 0

    def parse(self) -> List[COBOLDivision]:
        """Parse complete COBOL structure."""
        self._identify_divisions()
        self._identify_sections()
        self._identify_paragraphs()
        return self.divisions

    def _is_in_area_a(self, line: COBOLLine) -> bool:
        """Check if content starts in Area A (columns 8-11)."""
        if line.is_comment or line.is_blank:
            return False
        # In fixed format, Area A is the first 4 chars of content
        return bool(line.area_a.strip())

    def _identify_divisions(self):
        """Identify all DIVISION boundaries."""
        current_division = None
        division_start = 0

        for line in self.lines:
            if line.is_comment or line.is_blank:
                continue

            content_upper = line.content.upper().strip()

            # Check for DIVISION (must be in Area A)
            if self._is_in_area_a(line) and 'DIVISION' in content_upper:
                for div_name in self.DIVISION_KEYWORDS:
                    if content_upper.startswith(div_name) or f'{div_name} DIVISION' in content_upper:
                        # Save previous division
                        if current_division:
                            current_division.end_line = line.line_number - 1
                            current_division.content = self._extract_content(
                                division_start, line.line_number - 1
                            )
                            self.divisions.append(current_division)

                        current_division = COBOLDivision(
                            name=div_name,
                            content="",
                            start_line=line.line_number,
                            end_line=0
                        )
                        division_start = line.line_number

                        if div_name == 'PROCEDURE':
                            self.procedure_start_line = line.line_number
                        break

        # Save last division
        if current_division:
            current_division.end_line = len(self.lines)
            current_division.content = self._extract_content(division_start, len(self.lines))
            self.divisions.append(current_division)
            if current_division.name == 'PROCEDURE':
                self.procedure_end_line = len(self.lines)

    def _identify_sections(self):
        """Identify SECTION boundaries within divisions."""
        for division in self.divisions:
            current_section = None
            section_start = division.start_line

            for line in self.lines:
                if line.line_number < division.start_line or line.line_number > division.end_line:
                    continue
                if line.is_comment or line.is_blank:
                    continue

                content_upper = line.content.upper().strip()

                # Check for SECTION (must be in Area A)
                if self._is_in_area_a(line) and content_upper.endswith('SECTION') or content_upper.endswith('SECTION.'):
                    section_name = content_upper.replace('SECTION', '').replace('.', '').strip()

                    if section_name:
                        # Save previous section
                        if current_section:
                            current_section.end_line = line.line_number - 1
                            current_section.content = self._extract_content(
                                section_start, line.line_number - 1
                            )
                            division.sections.append(current_section)

                        current_section = COBOLSection(
                            name=section_name,
                            content="",
                            start_line=line.line_number,
                            end_line=0
                        )
                        section_start = line.line_number

            # Save last section
            if current_section:
                current_section.end_line = division.end_line
                current_section.content = self._extract_content(section_start, division.end_line)
                division.sections.append(current_section)

    def _identify_paragraphs(self):
        """Identify paragraphs in PROCEDURE DIVISION only."""
        procedure_div = next((d for d in self.divisions if d.name == 'PROCEDURE'), None)
        if not procedure_div:
            return

        # Pattern for paragraph name: starts in Area A, ends with period
        para_pattern = re.compile(r'^([A-Z0-9][\w-]*)\s*\.\s*$', re.IGNORECASE)

        current_paragraph = None
        para_start = procedure_div.start_line

        for line in self.lines:
            if line.line_number < procedure_div.start_line or line.line_number > procedure_div.end_line:
                continue
            if line.is_comment or line.is_blank:
                continue

            # Skip SECTION declarations
            if 'SECTION' in line.content.upper():
                continue

            # Check for paragraph (must start in Area A)
            if self._is_in_area_a(line):
                content_stripped = line.content.strip()
                match = para_pattern.match(content_stripped)

                if match:
                    para_name = match.group(1).upper()

                    # Skip COBOL reserved words
                    if para_name in ('PROCEDURE', 'DECLARATIVES', 'END'):
                        continue

                    # Save previous paragraph
                    if current_paragraph:
                        current_paragraph.end_line = line.line_number - 1
                        current_paragraph.content = self._extract_content(
                            para_start, line.line_number - 1
                        )
                        self._extract_paragraph_flow(current_paragraph)

                        # Add to appropriate section or division
                        self._add_paragraph_to_structure(current_paragraph, procedure_div)

                    current_paragraph = COBOLParagraph(
                        name=para_name,
                        start_line=line.line_number,
                        end_line=0,
                        content=""
                    )
                    para_start = line.line_number

        # Save last paragraph
        if current_paragraph:
            current_paragraph.end_line = procedure_div.end_line
            current_paragraph.content = self._extract_content(para_start, procedure_div.end_line)
            self._extract_paragraph_flow(current_paragraph)
            self._add_paragraph_to_structure(current_paragraph, procedure_div)

    def _add_paragraph_to_structure(self, paragraph: COBOLParagraph, division: COBOLDivision):
        """Add paragraph to appropriate section or directly to division."""
        for section in division.sections:
            if section.start_line <= paragraph.start_line <= section.end_line:
                section.paragraphs.append(paragraph)
                return
        # If no section contains it, add to first section or create default
        if division.sections:
            division.sections[0].paragraphs.append(paragraph)

    def _extract_paragraph_flow(self, paragraph: COBOLParagraph):
        """Extract PERFORM, GO TO, and CALL from paragraph content."""
        content = paragraph.content.upper()

        # PERFORM variants
        perform_patterns = [
            # PERFORM para-name [THRU para-name2]
            re.compile(r'PERFORM\s+([A-Z0-9][\w-]*)(?:\s+THRU\s+([A-Z0-9][\w-]*))?', re.IGNORECASE),
            # PERFORM para-name UNTIL condition
            re.compile(r'PERFORM\s+([A-Z0-9][\w-]*)\s+(?:WITH\s+TEST\s+(?:BEFORE|AFTER)\s+)?UNTIL\s+', re.IGNORECASE),
            # PERFORM para-name VARYING
            re.compile(r'PERFORM\s+([A-Z0-9][\w-]*)\s+VARYING\s+', re.IGNORECASE),
            # PERFORM para-name n TIMES
            re.compile(r'PERFORM\s+([A-Z0-9][\w-]*)\s+\d+\s+TIMES', re.IGNORECASE),
        ]

        for pattern in perform_patterns:
            for match in pattern.finditer(content):
                target = match.group(1)
                if target not in ('UNTIL', 'VARYING', 'WITH', 'TEST'):
                    paragraph.performs.append(target)
                    if len(match.groups()) > 1 and match.group(2):
                        paragraph.performs.append(f"{target}..{match.group(2)}")

        # GO TO
        goto_pattern = re.compile(r'GO\s+TO\s+([A-Z0-9][\w-]*)', re.IGNORECASE)
        for match in goto_pattern.finditer(content):
            paragraph.gotos.append(match.group(1))

        # CALL
        call_patterns = [
            re.compile(r"CALL\s+'([^']+)'", re.IGNORECASE),      # CALL 'PROGRAM-NAME'
            re.compile(r'CALL\s+"([^"]+)"', re.IGNORECASE),      # CALL "PROGRAM-NAME"
            re.compile(r'CALL\s+([A-Z0-9][\w-]*)', re.IGNORECASE),  # CALL WS-PROG-NAME
        ]
        for pattern in call_patterns:
            for match in pattern.finditer(content):
                paragraph.calls.append(match.group(1))

    def _extract_content(self, start_line: int, end_line: int) -> str:
        """Extract content between line numbers."""
        return '\n'.join(
            line.content for line in self.lines
            if start_line <= line.line_number <= end_line
            and not line.is_comment
        )

    def get_all_paragraphs(self) -> List[COBOLParagraph]:
        """Get flat list of all paragraphs."""
        paragraphs = []
        for division in self.divisions:
            for section in division.sections:
                paragraphs.extend(section.paragraphs)
        return paragraphs

    def get_control_flow(self) -> List[ControlFlowEntry]:
        """Extract complete control flow graph."""
        flow = []

        for para in self.get_all_paragraphs():
            for target in para.performs:
                if '..' in target:  # THRU
                    flow.append(ControlFlowEntry(
                        source=para.name,
                        target=target,
                        flow_type='PERFORM-THRU',
                        line_number=para.start_line
                    ))
                else:
                    flow.append(ControlFlowEntry(
                        source=para.name,
                        target=target,
                        flow_type='PERFORM',
                        line_number=para.start_line
                    ))

            for target in para.gotos:
                flow.append(ControlFlowEntry(
                    source=para.name,
                    target=target,
                    flow_type='GOTO',
                    line_number=para.start_line
                ))

            for target in para.calls:
                flow.append(ControlFlowEntry(
                    source=para.name,
                    target=target,
                    flow_type='CALL',
                    line_number=para.start_line
                ))

        return flow


# ============================================================================
# Enhanced Business Rule Extractor
# ============================================================================

class EnhancedBusinessRuleExtractor:
    """
    Extracts business rules with improved pattern detection.

    Improvements:
    1. Uses parsed structure instead of raw regex
    2. Identifies business pattern contexts
    3. Provides evidence for each identified pattern
    """

    BUSINESS_PATTERN_CONFIGS = {
        'VALIDATION': {
            'keywords': ['VALID', 'INVALID', 'ERROR', 'CHECK', 'VERIFY', 'VALIDATE'],
            'patterns': [
                r'IF\s+\S+\s*(?:=|NOT\s*=|IS\s+(?:NOT\s+)?(?:NUMERIC|ALPHABETIC|SPACES))',
                r'INSPECT\s+\S+\s+TALLYING',
            ],
            'description': 'Data validation logic'
        },
        'CALCULATION': {
            'keywords': ['COMPUTE', 'ADD', 'SUBTRACT', 'MULTIPLY', 'DIVIDE', 'TOTAL', 'SUM', 'CALC'],
            'patterns': [
                r'COMPUTE\s+\S+\s*=',
                r'ADD\s+.+\s+TO\s+',
                r'SUBTRACT\s+.+\s+FROM\s+',
            ],
            'description': 'Business calculations'
        },
        'FILE_IO': {
            'keywords': ['READ', 'WRITE', 'REWRITE', 'DELETE', 'START', 'OPEN', 'CLOSE'],
            'patterns': [
                r'READ\s+\S+\s+(?:INTO|AT\s+END)',
                r'WRITE\s+\S+\s+(?:FROM|AFTER|BEFORE)',
            ],
            'description': 'File processing operations'
        },
        'DATE_PROCESSING': {
            'keywords': ['DATE', 'YEAR', 'MONTH', 'DAY', 'YYMMDD', 'CCYYMMDD', 'TIMESTAMP'],
            'patterns': [
                r'FUNCTION\s+CURRENT-DATE',
                r'ACCEPT\s+\S+\s+FROM\s+DATE',
            ],
            'description': 'Date/time manipulation'
        },
        'ERROR_HANDLING': {
            'keywords': ['FILE-STATUS', 'SQLCODE', 'RETURN-CODE', 'ABEND', 'EXCEPTION'],
            'patterns': [
                r'IF\s+\S+-STATUS\s*(?:NOT\s*)?=',
                r'IF\s+SQLCODE\s*(?:NOT\s*)?=',
            ],
            'description': 'Error handling logic'
        },
        'DATABASE_ACCESS': {
            'keywords': ['EXEC', 'SQL', 'CURSOR', 'FETCH', 'INSERT', 'UPDATE', 'DELETE', 'SELECT'],
            'patterns': [
                r'EXEC\s+SQL',
                r'EXEC\s+CICS',
            ],
            'description': 'Database/CICS operations'
        },
        'STRING_MANIPULATION': {
            'keywords': ['STRING', 'UNSTRING', 'INSPECT', 'REPLACING', 'CONVERTING'],
            'patterns': [
                r'STRING\s+.+\s+DELIMITED\s+BY',
                r'UNSTRING\s+.+\s+DELIMITED\s+BY',
            ],
            'description': 'String processing'
        },
    }

    def __init__(self, parser: EnhancedCOBOLParser):
        self.parser = parser

    def extract_patterns(self) -> List[BusinessPattern]:
        """Extract all business patterns from parsed structure."""
        patterns = []

        for para in self.parser.get_all_paragraphs():
            content_upper = para.content.upper()

            for pattern_type, config in self.BUSINESS_PATTERN_CONFIGS.items():
                evidence = []

                # Check keywords
                for keyword in config['keywords']:
                    if keyword in content_upper:
                        # Extract context around keyword
                        idx = content_upper.find(keyword)
                        start = max(0, idx - 30)
                        end = min(len(para.content), idx + len(keyword) + 50)
                        evidence.append(para.content[start:end].strip())

                # Check regex patterns
                for regex in config['patterns']:
                    for match in re.finditer(regex, content_upper):
                        start = max(0, match.start() - 20)
                        end = min(len(para.content), match.end() + 30)
                        evidence.append(para.content[start:end].strip())

                if evidence:
                    patterns.append(BusinessPattern(
                        pattern_type=pattern_type,
                        description=config['description'],
                        source_location=f"{para.name} (lines {para.start_line}-{para.end_line})",
                        evidence=list(set(evidence))[:5],  # Limit evidence items
                        confidence=min(1.0, len(evidence) * 0.25)
                    ))

        return patterns

    def generate_context_for_llm(self) -> str:
        """Generate structured context to enhance LLM prompts."""
        output = []

        # Control Flow Summary
        flow = self.parser.get_control_flow()
        if flow:
            output.append("## Pre-Analyzed Control Flow:")
            perform_map: Dict[str, List[str]] = {}
            for entry in flow:
                if entry.flow_type.startswith('PERFORM'):
                    if entry.source not in perform_map:
                        perform_map[entry.source] = []
                    perform_map[entry.source].append(entry.target)

            for source, targets in list(perform_map.items())[:20]:  # Limit output
                output.append(f"- {source} -> {', '.join(targets)}")

        # Business Patterns
        patterns = self.extract_patterns()
        if patterns:
            output.append("\n## Identified Business Patterns:")
            pattern_groups: Dict[str, List[str]] = {}
            for p in patterns:
                if p.pattern_type not in pattern_groups:
                    pattern_groups[p.pattern_type] = []
                pattern_groups[p.pattern_type].append(p.source_location)

            for ptype, locations in pattern_groups.items():
                output.append(f"- {ptype}: Found in {', '.join(locations[:5])}")

        # CALL statements (external dependencies)
        calls = [e for e in flow if e.flow_type == 'CALL']
        if calls:
            output.append("\n## External Program Calls:")
            for call in calls[:10]:
                output.append(f"- CALL {call.target} from {call.source}")

        return '\n'.join(output)


# ============================================================================
# Enhanced Chunker with Data Context
# ============================================================================

class EnhancedCOBOLChunker:
    """
    Intelligent chunking that maintains DATA DIVISION context.

    Improvements:
    1. Includes DATA DIVISION summary with PROCEDURE chunks
    2. Respects semantic boundaries
    3. Provides chunk metadata for aggregation
    """

    def __init__(self, parser: EnhancedCOBOLParser, max_tokens: int = 6000):
        self.parser = parser
        self.max_tokens = max_tokens
        self.chars_per_token = 4

    def estimate_tokens(self, text: str) -> int:
        return len(text) // self.chars_per_token

    def create_chunks(self) -> List[Tuple[str, str, str, str]]:
        """
        Create chunks with DATA context.

        Returns:
            List of (chunk_id, chunk_type, chunk_content, data_context) tuples
        """
        chunks = []

        # Extract DATA DIVISION summary for context
        data_context = self._create_data_context()

        for division in self.parser.divisions:
            if division.name == 'PROCEDURE':
                # For PROCEDURE, chunk by section/paragraph
                chunks.extend(self._chunk_procedure_division(division, data_context))
            else:
                # Other divisions - include as single chunk if fits
                div_tokens = self.estimate_tokens(division.content)
                if div_tokens <= self.max_tokens:
                    chunks.append((
                        f"{division.name}_FULL",
                        division.name,
                        division.content,
                        ""  # No data context needed for DATA division itself
                    ))
                else:
                    # Split by sections
                    chunks.extend(self._chunk_by_sections(division))

        return chunks

    def _create_data_context(self) -> str:
        """Create a summary of DATA DIVISION for PROCEDURE chunks."""
        data_div = next((d for d in self.parser.divisions if d.name == 'DATA'), None)
        if not data_div:
            return ""

        context_parts = ["=== DATA DIVISION SUMMARY (for context) ==="]

        # Extract key 01-level items
        level_01_pattern = re.compile(r'^\s*01\s+([A-Z0-9][\w-]*)', re.MULTILINE | re.IGNORECASE)
        for match in level_01_pattern.finditer(data_div.content):
            context_parts.append(f"  01 {match.group(1)}")

        # Extract FD entries
        fd_pattern = re.compile(r'FD\s+([A-Z0-9][\w-]*)', re.IGNORECASE)
        for match in fd_pattern.finditer(data_div.content):
            context_parts.append(f"  FD {match.group(1)}")

        context_parts.append("=== END DATA SUMMARY ===\n")

        # Limit context size
        context = '\n'.join(context_parts)
        if self.estimate_tokens(context) > 500:
            context = context[:2000] + "\n... (truncated)"

        return context

    def _chunk_procedure_division(self, division: COBOLDivision,
                                   data_context: str) -> List[Tuple[str, str, str, str]]:
        """Chunk PROCEDURE DIVISION while preserving data context."""
        chunks = []

        if division.sections:
            current_content = []
            current_tokens = self.estimate_tokens(data_context)
            chunk_num = 1

            for section in division.sections:
                section_tokens = self.estimate_tokens(section.content)

                if current_tokens + section_tokens > self.max_tokens:
                    if current_content:
                        chunks.append((
                            f"PROCEDURE_PART{chunk_num}",
                            "PROCEDURE",
                            '\n'.join(current_content),
                            data_context
                        ))
                        chunk_num += 1
                        current_content = []
                        current_tokens = self.estimate_tokens(data_context)

                current_content.append(f"       {section.name} SECTION.\n{section.content}")
                current_tokens += section_tokens

            if current_content:
                chunks.append((
                    f"PROCEDURE_PART{chunk_num}",
                    "PROCEDURE",
                    '\n'.join(current_content),
                    data_context
                ))
        else:
            # No sections, use full content
            if self.estimate_tokens(division.content) <= self.max_tokens - self.estimate_tokens(data_context):
                chunks.append((
                    "PROCEDURE_FULL",
                    "PROCEDURE",
                    division.content,
                    data_context
                ))
            else:
                # Split by line count
                lines = division.content.split('\n')
                lines_per_chunk = (self.max_tokens * self.chars_per_token) // 80

                for i in range(0, len(lines), lines_per_chunk):
                    chunk_num = i // lines_per_chunk + 1
                    chunks.append((
                        f"PROCEDURE_PART{chunk_num}",
                        "PROCEDURE",
                        '\n'.join(lines[i:i + lines_per_chunk]),
                        data_context
                    ))

        return chunks

    def _chunk_by_sections(self, division: COBOLDivision) -> List[Tuple[str, str, str, str]]:
        """Chunk a division by its sections."""
        chunks = []

        for i, section in enumerate(division.sections, 1):
            section_tokens = self.estimate_tokens(section.content)

            if section_tokens <= self.max_tokens:
                chunks.append((
                    f"{division.name}_{section.name}",
                    division.name,
                    section.content,
                    ""
                ))
            else:
                # Split large section
                lines = section.content.split('\n')
                lines_per_chunk = (self.max_tokens * self.chars_per_token) // 80

                for j in range(0, len(lines), lines_per_chunk):
                    chunk_num = j // lines_per_chunk + 1
                    chunks.append((
                        f"{division.name}_{section.name}_P{chunk_num}",
                        division.name,
                        '\n'.join(lines[j:j + lines_per_chunk]),
                        ""
                    ))

        return chunks


# ============================================================================
# Utility Functions
# ============================================================================

def parse_cobol_file(file_path: str, copybook_dirs: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Parse a COBOL file and return comprehensive analysis.

    Args:
        file_path: Path to COBOL source file
        copybook_dirs: List of directories to search for copybooks

    Returns:
        Dictionary with parsed structure, control flow, business patterns
    """
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        source = f.read()

    # Resolve copybooks if dirs provided
    enriched_source = source
    resolved_copybooks = {}
    resolution_log = []

    if copybook_dirs:
        resolver = EnhancedCopybookResolver(copybook_dirs)
        enriched_source, resolved_copybooks = resolver.enrich_source(source)
        resolution_log = resolver.resolution_log

    # Parse structure
    parser = EnhancedCOBOLParser(enriched_source)
    divisions = parser.parse()

    # Extract business rules
    extractor = EnhancedBusinessRuleExtractor(parser)
    patterns = extractor.extract_patterns()
    llm_context = extractor.generate_context_for_llm()

    # Create chunks
    chunker = EnhancedCOBOLChunker(parser)
    chunks = chunker.create_chunks()

    return {
        'file_path': file_path,
        'original_source': source,
        'enriched_source': enriched_source,
        'resolved_copybooks': resolved_copybooks,
        'resolution_log': resolution_log,
        'divisions': [
            {
                'name': d.name,
                'start_line': d.start_line,
                'end_line': d.end_line,
                'sections': [
                    {
                        'name': s.name,
                        'start_line': s.start_line,
                        'end_line': s.end_line,
                        'paragraph_count': len(s.paragraphs)
                    }
                    for s in d.sections
                ]
            }
            for d in divisions
        ],
        'control_flow': [
            {
                'source': e.source,
                'target': e.target,
                'type': e.flow_type,
                'line': e.line_number
            }
            for e in parser.get_control_flow()
        ],
        'business_patterns': [
            {
                'type': p.pattern_type,
                'description': p.description,
                'location': p.source_location,
                'confidence': p.confidence
            }
            for p in patterns
        ],
        'llm_context': llm_context,
        'chunks': [
            {
                'id': c[0],
                'type': c[1],
                'content_length': len(c[2]),
                'has_data_context': bool(c[3])
            }
            for c in chunks
        ]
    }


if __name__ == '__main__':
    # Test with a sample file if run directly
    import sys
    if len(sys.argv) > 1:
        result = parse_cobol_file(sys.argv[1])
        print(f"Parsed: {result['file_path']}")
        print(f"Divisions: {[d['name'] for d in result['divisions']]}")
        print(f"Control flow entries: {len(result['control_flow'])}")
        print(f"Business patterns: {len(result['business_patterns'])}")
        print(f"Chunks: {len(result['chunks'])}")
