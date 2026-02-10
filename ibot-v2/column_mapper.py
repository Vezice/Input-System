"""
iBot v2 Column Mapper

Maps source file headers to standard column names.
Handles:
- Direct header matching (case-insensitive)
- Column aliases (same data, different header names)
- Special rules: COALESCE_EXACT, COALESCE_STARTS_WITH, SUM_STARTS_WITH
- Dynamic headers (Nama Variasi 1, Option 1 Name, etc.)

This enables v2 to handle files with different column orders,
matching v1's flexibility.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ColumnRule:
    """A special column mapping rule."""
    standard_column: str
    action: str  # COALESCE_EXACT, COALESCE_STARTS_WITH, SUM_STARTS_WITH
    replacements: List[str] = field(default_factory=list)


def normalize_header(header: str) -> str:
    """Normalize header for comparison (lowercase, trimmed)."""
    if not header:
        return ""
    return str(header).lower().strip()


def build_source_header_map(headers: List[str]) -> Dict[str, List[int]]:
    """
    Build a map of normalized header names to their column indices.

    Handles duplicate headers by storing all indices in a list.

    Args:
        headers: List of header names from source file

    Returns:
        Dict mapping normalized header name to list of column indices
    """
    header_map: Dict[str, List[int]] = {}

    for idx, header in enumerate(headers):
        key = normalize_header(header)
        if not key:
            continue

        if key not in header_map:
            header_map[key] = []
        header_map[key].append(idx)

    return header_map


def detect_dynamic_headers(headers: List[str]) -> List[str]:
    """
    Detect dynamic/variable headers like variation names and images.

    Patterns recognized:
    - Indonesian: "Nama Variasi 1", "Foto Variasi 1", etc.
    - English: "Option 1 Name", "Option 1 Image", etc.
    - Special: "Foto Panduan Ukuran"

    Returns headers sorted in interleaved order:
    Foto Panduan Ukuran, Name 1, Image 1, Name 2, Image 2, ...
    """
    dynamic_headers = []
    name_headers = []  # (number, header)
    image_headers = []  # (number, header)
    special_headers = []

    for header in headers:
        header_lower = normalize_header(header)

        # Check for size guide photo (special, appears first)
        if "foto panduan ukuran" in header_lower or "size guide" in header_lower:
            special_headers.append(header)
            continue

        # Indonesian patterns: "Nama Variasi X", "Foto Variasi X"
        nama_match = re.search(r'nama\s*variasi\s*(\d+)', header_lower)
        foto_match = re.search(r'foto\s*variasi\s*(\d+)', header_lower)

        # English patterns: "Option X Name", "Option X Image"
        option_name_match = re.search(r'option\s*(\d+)\s*name', header_lower)
        option_image_match = re.search(r'option\s*(\d+)\s*image', header_lower)

        if nama_match:
            name_headers.append((int(nama_match.group(1)), header))
        elif foto_match:
            image_headers.append((int(foto_match.group(1)), header))
        elif option_name_match:
            name_headers.append((int(option_name_match.group(1)), header))
        elif option_image_match:
            image_headers.append((int(option_image_match.group(1)), header))

    # Sort by number
    name_headers.sort(key=lambda x: x[0])
    image_headers.sort(key=lambda x: x[0])

    # Build interleaved list: special first, then name/image pairs
    dynamic_headers.extend(special_headers)

    # Interleave names and images (max 15 pairs like v1)
    max_pairs = 15
    name_dict = {num: hdr for num, hdr in name_headers}
    image_dict = {num: hdr for num, hdr in image_headers}

    all_numbers = sorted(set(name_dict.keys()) | set(image_dict.keys()))[:max_pairs]

    for num in all_numbers:
        if num in name_dict:
            dynamic_headers.append(name_dict[num])
        if num in image_dict:
            dynamic_headers.append(image_dict[num])

    return dynamic_headers


def apply_coalesce_exact(
    source_header_map: Dict[str, List[int]],
    working_map: Dict[str, List[int]],
    replacements: List[str],
) -> Optional[int]:
    """
    Apply COALESCE_EXACT rule: try each replacement in order, use first that exists.

    Args:
        source_header_map: Original header map (for checking existence)
        working_map: Working map (for claiming columns, modified in place)
        replacements: List of alternative header names to try

    Returns:
        Column index if found, None otherwise
    """
    for replacement in replacements:
        key = normalize_header(replacement)
        if key in working_map and working_map[key]:
            # Claim this column (first-come-first-serve)
            return working_map[key].pop(0)
    return None


def apply_coalesce_starts_with(
    source_header_map: Dict[str, List[int]],
    working_map: Dict[str, List[int]],
    patterns: List[str],
) -> Optional[int]:
    """
    Apply COALESCE_STARTS_WITH rule: find first header starting with pattern.

    Args:
        source_header_map: Original header map
        working_map: Working map (modified in place)
        patterns: List of prefix patterns to try

    Returns:
        Column index if found, None otherwise
    """
    for pattern in patterns:
        pattern_lower = normalize_header(pattern)
        for key in list(working_map.keys()):
            if key.startswith(pattern_lower) and working_map[key]:
                return working_map[key].pop(0)
    return None


def apply_sum_starts_with(
    source_row: List[Any],
    source_header_map: Dict[str, List[int]],
    patterns: List[str],
) -> str:
    """
    Apply SUM_STARTS_WITH rule: sum all columns matching pattern.

    Args:
        source_row: The data row
        source_header_map: Header map
        patterns: List of prefix patterns

    Returns:
        Sum as string, or empty string if no matches
    """
    total = 0
    found_any = False

    for pattern in patterns:
        pattern_lower = normalize_header(pattern)
        for key, indices in source_header_map.items():
            if key.startswith(pattern_lower):
                for idx in indices:
                    if idx < len(source_row):
                        value = source_row[idx]
                        if value is not None:
                            try:
                                # Handle Indonesian number format
                                value_str = str(value).strip()
                                if value_str and value_str not in ('-', 'N/A', '#N/A'):
                                    # Remove thousand separators, handle decimal
                                    value_str = value_str.replace('.', '').replace(',', '.')
                                    total += float(value_str)
                                    found_any = True
                            except (ValueError, TypeError):
                                pass

    if found_any:
        # Return as integer if whole number
        if total == int(total):
            return str(int(total))
        return str(total)
    return ""


class ColumnMapper:
    """
    Maps source file columns to standard column order.

    Usage:
        mapper = ColumnMapper(standard_headers, column_rules)
        normalized_headers, normalized_rows = mapper.map_file(source_headers, source_rows)
    """

    def __init__(
        self,
        standard_headers: List[str],
        column_rules: Optional[List[ColumnRule]] = None,
        include_dynamic: bool = True,
    ):
        """
        Initialize column mapper.

        Args:
            standard_headers: List of expected standard header names (from Type Validation)
            column_rules: List of special mapping rules (from Unique Column)
            include_dynamic: Whether to detect and include dynamic headers
        """
        self.standard_headers = standard_headers
        self.column_rules = column_rules or []
        self.include_dynamic = include_dynamic

        # Build rules lookup by standard column name
        self.rules_by_column: Dict[str, ColumnRule] = {
            rule.standard_column: rule for rule in self.column_rules
        }

    def map_file(
        self,
        source_headers: List[str],
        source_rows: List[List[Any]],
    ) -> Tuple[List[str], List[Dict[str, Any]]]:
        """
        Map source file data to standard column order.

        Args:
            source_headers: Headers from source file
            source_rows: Data rows from source file

        Returns:
            Tuple of (output_headers, output_rows)
            - output_headers: Standard headers in correct order
            - output_rows: Data rows as dicts with standard header keys
        """
        # Build source header map
        source_header_map = build_source_header_map(source_headers)

        logger.debug(
            f"Mapping columns: {len(source_headers)} source -> {len(self.standard_headers)} standard",
            source_count=len(source_headers),
            standard_count=len(self.standard_headers),
        )

        # Detect dynamic headers if enabled
        dynamic_headers = []
        if self.include_dynamic:
            dynamic_headers = detect_dynamic_headers(source_headers)

        # Build final output headers: standard + dynamic (excluding duplicates)
        output_headers = list(self.standard_headers)
        standard_set = {normalize_header(h) for h in self.standard_headers}

        for dh in dynamic_headers:
            if normalize_header(dh) not in standard_set:
                output_headers.append(dh)
                standard_set.add(normalize_header(dh))

        # Pre-calculate column mapping for each output header
        # This is done once, then applied to all rows
        column_mapping = self._calculate_mapping(
            output_headers,
            source_header_map,
        )

        # Log mapping results
        mapped_count = sum(1 for m in column_mapping if m['source_idx'] is not None)
        logger.info(
            f"Column mapping: {mapped_count}/{len(output_headers)} headers mapped",
            mapped=mapped_count,
            total=len(output_headers),
        )

        # Map each row
        output_rows = []
        for source_row in source_rows:
            output_row = self._map_row(
                source_row,
                output_headers,
                column_mapping,
                source_header_map,
            )
            output_rows.append(output_row)

        return output_headers, output_rows

    def _calculate_mapping(
        self,
        output_headers: List[str],
        source_header_map: Dict[str, List[int]],
    ) -> List[Dict[str, Any]]:
        """
        Pre-calculate column mapping for efficiency.

        Returns list of mapping info for each output header:
        - source_idx: Direct source column index (or None)
        - rule: Special rule to apply (or None)
        """
        # Create working copy of header map (for first-come-first-serve allocation)
        working_map = {k: list(v) for k, v in source_header_map.items()}

        mappings = []

        for header in output_headers:
            mapping = {
                'header': header,
                'source_idx': None,
                'rule': None,
            }

            header_key = normalize_header(header)

            # Try direct match first
            if header_key in working_map and working_map[header_key]:
                mapping['source_idx'] = working_map[header_key].pop(0)

            # If no direct match, check for special rule
            elif header in self.rules_by_column:
                rule = self.rules_by_column[header]
                mapping['rule'] = rule

                if rule.action == 'COALESCE_EXACT':
                    idx = apply_coalesce_exact(
                        source_header_map,
                        working_map,
                        rule.replacements,
                    )
                    mapping['source_idx'] = idx

                elif rule.action == 'COALESCE_STARTS_WITH':
                    idx = apply_coalesce_starts_with(
                        source_header_map,
                        working_map,
                        rule.replacements,
                    )
                    mapping['source_idx'] = idx

                # SUM_STARTS_WITH is handled per-row, not pre-calculated

            mappings.append(mapping)

        return mappings

    def _map_row(
        self,
        source_row: List[Any],
        output_headers: List[str],
        column_mapping: List[Dict[str, Any]],
        source_header_map: Dict[str, List[int]],
    ) -> Dict[str, Any]:
        """Map a single source row to output format."""
        output_row = {}

        for i, mapping in enumerate(column_mapping):
            header = mapping['header']
            source_idx = mapping['source_idx']
            rule = mapping['rule']

            value = ""

            if source_idx is not None:
                # Direct mapping
                if source_idx < len(source_row):
                    value = source_row[source_idx]
                    if value is None:
                        value = ""
                    else:
                        value = str(value).strip()

            elif rule and rule.action == 'SUM_STARTS_WITH':
                # Sum multiple columns
                value = apply_sum_starts_with(
                    source_row,
                    source_header_map,
                    rule.replacements,
                )

            output_row[header] = value

        return output_row


def create_mapper_for_category(
    category_name: str,
    standard_headers: List[str],
    column_aliases: Optional[Dict[str, List[str]]] = None,
) -> ColumnMapper:
    """
    Create a ColumnMapper for a specific category.

    Args:
        category_name: Name of the category (e.g., "BA Produk SHO")
        standard_headers: List of standard headers from Type Validation
        column_aliases: Dict of standard_column -> [aliases] from Unique Column

    Returns:
        Configured ColumnMapper instance
    """
    rules = []

    if column_aliases:
        for standard_col, aliases in column_aliases.items():
            # Determine action type based on aliases
            # If any alias ends with *, it's a STARTS_WITH pattern
            starts_with_aliases = [a[:-1] for a in aliases if a.endswith('*')]
            exact_aliases = [a for a in aliases if not a.endswith('*')]

            if starts_with_aliases:
                # Check if it looks like a SUM pattern (multiple columns to sum)
                if any('sum' in standard_col.lower() or 'total' in standard_col.lower()
                       for _ in [1]):  # Placeholder condition
                    rules.append(ColumnRule(
                        standard_column=standard_col,
                        action='SUM_STARTS_WITH',
                        replacements=starts_with_aliases,
                    ))
                else:
                    rules.append(ColumnRule(
                        standard_column=standard_col,
                        action='COALESCE_STARTS_WITH',
                        replacements=starts_with_aliases,
                    ))

            if exact_aliases:
                rules.append(ColumnRule(
                    standard_column=standard_col,
                    action='COALESCE_EXACT',
                    replacements=exact_aliases,
                ))

    # Determine if this category should include dynamic headers
    include_dynamic = any(cat in category_name for cat in [
        "BA Produk",
        "Informasi Media",
    ])

    return ColumnMapper(
        standard_headers=standard_headers,
        column_rules=rules,
        include_dynamic=include_dynamic,
    )
