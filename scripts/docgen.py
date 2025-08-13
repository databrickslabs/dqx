#!/usr/bin/env python3
"""
Script to generate comprehensive markdown documentation from Python module docstrings.

This script analyzes Python modules and generates a single markdown file with
structured documentation including function signatures, parameters, and metadata.
"""

import ast
import importlib
import inspect
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class DocstringInfo:
    """Container for docstring information."""
    
    source_file_name: str
    source_file_path: str
    module_name: str
    class_name: Optional[str]
    function_name: str
    method_type: Optional[str]  # 'classmethod', 'staticmethod', 'instancemethod', or None
    doc_body: str
    parameters: List[Dict[str, Any]]
    return_info: Optional[Dict[str, Any]]
    docstring: str
    line_start: int
    line_end: int


class MarkdownDocGenerator:
    """Generates markdown documentation from collected docstrings."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.collected_docstrings: List[DocstringInfo] = []
        self.stats = {
            'modules_processed': 0,
            'classes_processed': 0,
            'functions_processed': 0,
            'docstrings_found': 0,
            'errors': 0
        }
    
    def _get_relative_path(self, file_path: str) -> str:
        """Get relative path from project root."""
        try:
            return str(Path(file_path).relative_to(self.project_root))
        except ValueError:
            return file_path
    
    def _get_method_type(self, func: Any, class_obj: Any = None) -> Optional[str]:
        """Determine if a method is classmethod, staticmethod, or instancemethod."""
        if class_obj is None:
            return None
        
        # Check if it's a classmethod
        if isinstance(inspect.getattr_static(class_obj, func.__name__, None), classmethod):
            return "classmethod"
        
        # Check if it's a staticmethod
        if isinstance(inspect.getattr_static(class_obj, func.__name__, None), staticmethod):
            return "staticmethod"
        
        # Check if it's a regular method (has 'self' parameter)
        try:
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            if params and params[0] in ('self', 'cls'):
                return "instancemethod"
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _extract_parameters(self, func: Any) -> List[Dict[str, Any]]:
        """Extract parameter information from a function."""
        try:
            sig = inspect.signature(func)
            parameters = []
            
            for param_name, param in sig.parameters.items():
                param_info = {
                    "name": param_name,
                    "annotation": str(param.annotation) if param.annotation != inspect.Parameter.empty else None,
                    "default": str(param.default) if param.default != inspect.Parameter.empty else None,
                    "kind": param.kind.name
                }
                parameters.append(param_info)
            
            return parameters
        except (ValueError, TypeError):
            return []
    
    def _extract_return_info(self, func: Any) -> Optional[Dict[str, Any]]:
        """Extract return type information from a function."""
        try:
            sig = inspect.signature(func)
            if sig.return_annotation != inspect.Signature.empty:
                return {
                    "annotation": str(sig.return_annotation),
                    "description": None  # Could be parsed from docstring
                }
        except (ValueError, TypeError):
            pass
        return None
    
    def _get_source_lines(self, obj: Any) -> tuple[int, int]:
        """Get start and end line numbers for an object."""
        try:
            source_lines = inspect.getsourcelines(obj)
            line_start = source_lines[1]
            line_end = line_start + len(source_lines[0]) - 1
            return line_start, line_end
        except (OSError, TypeError):
            return 0, 0
    
    def _escape_markdown(self, text: str) -> str:
        """Escape special markdown characters in text."""
        if not text:
            return ""
        # Escape MDX/JSX template syntax that could cause issues
        text = text.replace('{{', '&#123;&#123;')
        text = text.replace('}}', '&#125;&#125;')
        # Escape markdown special characters
        text = re.sub(r'([*_`\[\]{}()#+\-.!|\\])', r'\\\1', text)
        return text
    
    def _clean_type_annotation(self, annotation: str) -> str:
        """Clean up type annotations to avoid MDX parsing issues."""
        if not annotation:
            return ""
        
        # Replace problematic patterns in type annotations
        # Convert <class 'str'> to str
        annotation = re.sub(r"<class '([^']+)'>", r'\1', annotation)
        
        # Convert <class 'module.Class'> to module.Class
        annotation = re.sub(r"<class '([^']+)'>", r'\1', annotation)
        
        # Handle other <...> patterns that might contain quotes
        annotation = re.sub(r"<([^<>]*)'([^<>]*)'([^<>]*)>", r'&lt;\1"\2"\3&gt;', annotation)
        
        # Escape any remaining single quotes that might cause issues
        annotation = annotation.replace("'", "&#x27;")
        
        return annotation
    
    def _format_signature(self, func_name: str, parameters: List[Dict[str, Any]], 
                         return_info: Optional[Dict[str, Any]], method_type: Optional[str] = None) -> str:
        """Format function signature for markdown."""
        params_str = []
        for param in parameters:
            param_str = param['name']
            if param['annotation']:
                clean_annotation = self._clean_type_annotation(param['annotation'])
                param_str += f": {clean_annotation}"
            if param['default'] is not None:
                param_str += f" = {param['default']}"
            params_str.append(param_str)
        
        signature = f"{func_name}({', '.join(params_str)})"
        if return_info and return_info['annotation']:
            clean_return_annotation = self._clean_type_annotation(return_info['annotation'])
            signature += f" -> {clean_return_annotation}"
        
        if method_type:
            signature = f"@{method_type}\n{signature}"
        
        return signature
    
    def _process_function(self, func: Any, module_name: str, class_obj: Any = None, 
                         class_name: Optional[str] = None) -> Optional[DocstringInfo]:
        """Process a function and extract its docstring information."""
        try:
            self.stats['functions_processed'] += 1
            
            # Skip if no docstring
            docstring = inspect.getdoc(func)
            if not docstring:
                return None
            
            logger.debug(f"📝 Processing function: {func.__name__}")
            self.stats['docstrings_found'] += 1
            
            # Get source file information
            source_file = inspect.getfile(func)
            source_file_name = Path(source_file).name
            source_file_path = self._get_relative_path(source_file)
            
            # Get method type
            method_type = self._get_method_type(func, class_obj)
            
            # Extract parameters and return info
            parameters = self._extract_parameters(func)
            return_info = self._extract_return_info(func)
            
            # Get source line numbers
            line_start, line_end = self._get_source_lines(func)
            
            # Create docstring info
            docstring_info = DocstringInfo(
                source_file_name=source_file_name,
                source_file_path=source_file_path,
                module_name=module_name,
                class_name=class_name,
                function_name=func.__name__,
                method_type=method_type,
                doc_body=docstring,
                parameters=parameters,
                return_info=return_info,
                docstring=docstring,
                line_start=line_start,
                line_end=line_end
            )
            
            return docstring_info
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Error processing function {func.__name__}: {e}")
            return None
    
    def _process_class(self, class_obj: Any, module_name: str) -> List[DocstringInfo]:
        """Process a class and extract docstring information for it and its methods."""
        self.stats['classes_processed'] += 1
        logger.info(f"🏗️  Processing class: {class_obj.__name__}")
        
        docstrings = []
        
        # Process class docstring
        class_docstring = inspect.getdoc(class_obj)
        if class_docstring:
            try:
                source_file = inspect.getfile(class_obj)
                source_file_name = Path(source_file).name
                source_file_path = self._get_relative_path(source_file)
                line_start, line_end = self._get_source_lines(class_obj)
                
                class_docstring_info = DocstringInfo(
                    source_file_name=source_file_name,
                    source_file_path=source_file_path,
                    module_name=module_name,
                    class_name=class_obj.__name__,
                    function_name="__class__",
                    method_type=None,
                    doc_body=class_docstring,
                    parameters=[],
                    return_info=None,
                    docstring=class_docstring,
                    line_start=line_start,
                    line_end=line_end
                )
                docstrings.append(class_docstring_info)
            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"❌ Error processing class {class_obj.__name__}: {e}")
        
        # Process methods
        methods = [(method_name, method) for method_name, method in inspect.getmembers(class_obj, inspect.isfunction)
                   if not method_name.startswith('_') or method_name in ['__init__', '__new__']]
        
        if methods:
            logger.debug(f"     🔧 Processing {len(methods)} methods in {class_obj.__name__}")
            for method_name, method in methods:
                method_info = self._process_function(method, module_name, class_obj, class_obj.__name__)
                if method_info:
                    docstrings.append(method_info)
        
        # Process static methods and class methods
        for method_name, method in inspect.getmembers(class_obj):
            if isinstance(method, staticmethod):
                if hasattr(method, '__func__'):
                    method_info = self._process_function(method.__func__, module_name, class_obj, class_obj.__name__)
                    if method_info:
                        docstrings.append(method_info)
            elif isinstance(method, classmethod):
                if hasattr(method, '__func__'):
                    method_info = self._process_function(method.__func__, module_name, class_obj, class_obj.__name__)
                    if method_info:
                        docstrings.append(method_info)
            elif (callable(method) and not method_name.startswith('_') and 
                  not inspect.isfunction(method) and not inspect.ismethod(method)):
                # Handle other callable objects that might have docstrings
                try:
                    method_info = self._process_function(method, module_name, class_obj, class_obj.__name__)
                    if method_info:
                        docstrings.append(method_info)
                except Exception:
                    # Skip if we can't process this callable
                    pass
        
        return docstrings
    
    def _process_module(self, module: Any, module_name: str) -> List[DocstringInfo]:
        """Process a module and extract all docstring information."""
        self.stats['modules_processed'] += 1
        logger.info(f"📦 Processing module: {module_name}")
        
        docstrings = []
        
        # Process module-level functions
        module_functions = [func for func_name, func in inspect.getmembers(module, inspect.isfunction) 
                           if func.__module__ == module_name]
        
        if module_functions:
            logger.info(f"   🔧 Found {len(module_functions)} functions")
            for func in module_functions:
                func_info = self._process_function(func, module_name)
                if func_info:
                    docstrings.append(func_info)
        
        # Process classes
        module_classes = [class_obj for class_name, class_obj in inspect.getmembers(module, inspect.isclass)
                         if class_obj.__module__ == module_name]
        
        if module_classes:
            logger.info(f"   🏗️  Found {len(module_classes)} classes")
            for i, class_obj in enumerate(module_classes, 1):
                logger.debug(f"     📍 Processing class {i}/{len(module_classes)}: {class_obj.__name__}")
                class_docstrings = self._process_class(class_obj, module_name)
                docstrings.extend(class_docstrings)
        
        logger.info(f"   ✅ Collected {len(docstrings)} docstrings from {module_name}")
        return docstrings
    
    def collect_from_module(self, module_name: str) -> List[DocstringInfo]:
        """Collect docstrings from a given module."""
        logger.info(f"🚀 Starting docstring collection from module: {module_name}")
        
        try:
            # Import the module
            logger.info(f"📥 Importing module: {module_name}")
            module = importlib.import_module(module_name)
            
            # Process the module
            docstrings = self._process_module(module, module_name)
            
            # Also process submodules if it's a package
            if hasattr(module, '__path__'):
                logger.info(f"📁 Discovering submodules in package: {module_name}")
                try:
                    import pkgutil
                    submodules = list(pkgutil.iter_modules(module.__path__, module_name + "."))
                    logger.info(f"   🔍 Found {len(submodules)} submodules")
                    
                    for i, (importer, submodule_name, ispkg) in enumerate(submodules, 1):
                        try:
                            logger.debug(f"     📍 Processing submodule {i}/{len(submodules)}: {submodule_name}")
                            submodule = importlib.import_module(submodule_name)
                            sub_docstrings = self._process_module(submodule, submodule_name)
                            docstrings.extend(sub_docstrings)
                        except Exception as e:
                            self.stats['errors'] += 1
                            logger.error(f"❌ Error processing submodule {submodule_name}: {e}")
                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"❌ Error iterating submodules: {e}")
            
            logger.info(f"🎉 Successfully collected {len(docstrings)} docstrings from {module_name}")
            return docstrings
            
        except ImportError as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Error importing module {module_name}: {e}")
            return []
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Error processing module {module_name}: {e}")
            return []

    def _generate_markdown_content(self, docstrings: List[DocstringInfo], module_name: str) -> str:
        """Generate markdown content from collected docstrings."""
        logger.info(f"📝 Generating markdown content for {len(docstrings)} docstrings")
        
        # Group docstrings by module and class
        modules = {}
        for ds in docstrings:
            if ds.module_name not in modules:
                modules[ds.module_name] = {'functions': [], 'classes': {}}
            
            if ds.class_name and ds.function_name != '__class__':
                # This is a method
                if ds.class_name not in modules[ds.module_name]['classes']:
                    modules[ds.module_name]['classes'][ds.class_name] = {'docstring': None, 'methods': []}
                modules[ds.module_name]['classes'][ds.class_name]['methods'].append(ds)
            elif ds.class_name and ds.function_name == '__class__':
                # This is a class docstring
                if ds.class_name not in modules[ds.module_name]['classes']:
                    modules[ds.module_name]['classes'][ds.class_name] = {'docstring': None, 'methods': []}
                modules[ds.module_name]['classes'][ds.class_name]['docstring'] = ds
            else:
                # This is a module-level function
                modules[ds.module_name]['functions'].append(ds)
        
        # Generate markdown
        md_content = []
        
        # Header
        md_content.append(f"# {module_name} API Reference")
        md_content.append("")
        md_content.append('<div class="api-reference">')
        md_content.append("")
        
        # Note: Table of contents is handled by Docusaurus automatically
        
        # Generate content for each module
        for mod_name in sorted(modules.keys()):
            mod_data = modules[mod_name]
            
            md_content.append(f"## {mod_name}")
            md_content.append("")
            md_content.append(f'<div class="module" data-module="{mod_name}">')
            
            # Module-level functions
            if mod_data['functions']:
                md_content.append("### Functions")
                md_content.append("")
                md_content.append('<div class="functions-section">')
                
                for func in sorted(mod_data['functions'], key=lambda x: x.function_name):
                    md_content.append(f"#### {func.function_name}")
                    md_content.append("")
                    md_content.append(f'<div class="function" data-function="{func.function_name}">')
                    
                    # Source info
                    md_content.append(f'<div class="source-info">')
                    md_content.append(f"**Source:** `{func.source_file_path}:{func.line_start}-{func.line_end}`")
                    md_content.append("</div>")
                    md_content.append("")
                    
                    # Function signature
                    signature = self._format_signature(func.function_name, func.parameters, func.return_info, func.method_type)
                    md_content.append('<div class="signature">')
                    md_content.append("```python")
                    md_content.append(signature)
                    md_content.append("```")
                    md_content.append("</div>")
                    md_content.append("")
                    
                    # Docstring
                    md_content.append('<div class="docstring">')
                    md_content.append(self._escape_markdown(func.docstring))
                    md_content.append("</div>")
                    
                    # Parameters
                    if func.parameters:
                        md_content.append("")
                        md_content.append('<div class="parameters">')
                        md_content.append("**Parameters:**")
                        md_content.append("")
                        for param in func.parameters:
                            param_line = f"- `{param['name']}`"
                            if param['annotation']:
                                clean_annotation = self._clean_type_annotation(param['annotation'])
                                param_line += f" ({clean_annotation})"
                            if param['default'] is not None:
                                param_line += f", default: `{param['default']}`"
                            md_content.append(param_line)
                        md_content.append("</div>")
                    
                    # Return type
                    if func.return_info:
                        md_content.append("")
                        md_content.append('<div class="returns">')
                        clean_return_annotation = self._clean_type_annotation(func.return_info['annotation'])
                        md_content.append(f"**Returns:** `{clean_return_annotation}`")
                        md_content.append("</div>")
                    
                    md_content.append("")
                    md_content.append("</div>")
                    md_content.append("")
                
                md_content.append("</div>")
                md_content.append("")
            
            # Classes
            if mod_data['classes']:
                md_content.append("### Classes")
                md_content.append("")
                md_content.append('<div class="classes-section">')
                
                for class_name in sorted(mod_data['classes'].keys()):
                    class_data = mod_data['classes'][class_name]
                    
                    md_content.append(f"#### {class_name}")
                    md_content.append("")
                    md_content.append(f'<div class="class" data-class="{class_name}">')
                    
                    # Class docstring
                    if class_data['docstring']:
                        ds = class_data['docstring']
                        md_content.append(f'<div class="source-info">')
                        md_content.append(f"**Source:** `{ds.source_file_path}:{ds.line_start}-{ds.line_end}`")
                        md_content.append("</div>")
                        md_content.append("")
                        
                        md_content.append('<div class="class-docstring">')
                        md_content.append(self._escape_markdown(ds.docstring))
                        md_content.append("</div>")
                        md_content.append("")
                    
                    # Methods
                    if class_data['methods']:
                        md_content.append("##### Methods")
                        md_content.append("")
                        md_content.append('<div class="methods-section">')
                        
                        for method in sorted(class_data['methods'], key=lambda x: x.function_name):
                            md_content.append(f"###### {method.function_name}")
                            md_content.append("")
                            md_content.append(f'<div class="method" data-method="{method.function_name}">')
                            
                            # Method signature
                            signature = self._format_signature(method.function_name, method.parameters, method.return_info, method.method_type)
                            md_content.append('<div class="signature">')
                            md_content.append("```python")
                            md_content.append(signature)
                            md_content.append("```")
                            md_content.append("</div>")
                            md_content.append("")
                            
                            # Docstring
                            md_content.append('<div class="docstring">')
                            md_content.append(self._escape_markdown(method.docstring))
                            md_content.append("</div>")
                            
                            # Parameters
                            if method.parameters:
                                md_content.append("")
                                md_content.append('<div class="parameters">')
                                md_content.append("**Parameters:**")
                                md_content.append("")
                                for param in method.parameters:
                                    param_line = f"- `{param['name']}`"
                                    if param['annotation']:
                                        clean_annotation = self._clean_type_annotation(param['annotation'])
                                        param_line += f" ({clean_annotation})"
                                    if param['default'] is not None:
                                        param_line += f", default: `{param['default']}`"
                                    md_content.append(param_line)
                                md_content.append("</div>")
                            
                            # Return type
                            if method.return_info:
                                md_content.append("")
                                md_content.append('<div class="returns">')
                                clean_return_annotation = self._clean_type_annotation(method.return_info['annotation'])
                                md_content.append(f"**Returns:** `{clean_return_annotation}`")
                                md_content.append("</div>")
                            
                            md_content.append("")
                            md_content.append("</div>")
                            md_content.append("")
                        
                        md_content.append("</div>")
                    
                    md_content.append("</div>")
                    md_content.append("")
                
                md_content.append("</div>")
            
            md_content.append("</div>")
            md_content.append("")
        
        md_content.append("</div>")
        
        return "\n".join(md_content)


def generate_markdown_docs(module_name: str, output_file_path: Path) -> None:
    """
    Generate markdown documentation from a given module and save to a file.
    
    Args:
        module_name: Name of the module to analyze (e.g., 'databricks.labs.dqx')
        output_file_path: Path where to save the generated markdown documentation
    """
    start_time = time.time()
    
    logger.info(f"🌟 Starting DQX Markdown Documentation Generator")
    logger.info(f"📍 Target module: {module_name}")
    logger.info(f"📁 Output file: {output_file_path}")
    
    # Get project root (assume script is in scripts/ directory)
    project_root = Path(__file__).parent.parent
    logger.info(f"🏠 Project root: {project_root}")
    
    # Create generator
    generator = MarkdownDocGenerator(project_root)
    
    # Collect docstrings
    docstrings = generator.collect_from_module(module_name)
    
    # Generate markdown content
    markdown_content = generator._generate_markdown_content(docstrings, module_name)
    
    # Save to file
    logger.info(f"💾 Saving markdown documentation to {output_file_path}")
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    # Print summary statistics
    elapsed_time = time.time() - start_time
    logger.info(f"📊 Generation Summary:")
    logger.info(f"   📦 Modules processed: {generator.stats['modules_processed']}")
    logger.info(f"   🏗️  Classes processed: {generator.stats['classes_processed']}")
    logger.info(f"   🔧 Functions processed: {generator.stats['functions_processed']}")
    logger.info(f"   📝 Docstrings found: {generator.stats['docstrings_found']}")
    logger.info(f"   ❌ Errors encountered: {generator.stats['errors']}")
    logger.info(f"   💾 Output file size: {output_file_path.stat().st_size / 1024:.1f} KB")
    logger.info(f"   ⏱️  Execution time: {elapsed_time:.2f} seconds")
    
    logger.info(f"✅ Successfully completed! Generated markdown documentation at {output_file_path}")


def main():
    """Main function for command-line usage."""
    import argparse
    
    # Print header
    print("🚀 DQX Markdown Documentation Generator")
    print("=" * 50)
    
    parser = argparse.ArgumentParser(
        description="Generate comprehensive markdown documentation from Python modules",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/docgen.py databricks.labs.dqx docs/generated/api-reference.md
    python scripts/docgen.py databricks.labs.dqx.engine /tmp/engine_docs.md --verbose
        """
    )
    
    parser.add_argument("module_name", help="Name of the module to analyze (e.g., 'databricks.labs.dqx')")
    parser.add_argument("output_file", help="Output file path for generated markdown documentation")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set log level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("🔍 Verbose logging enabled")
    
    output_path = Path(args.output_file)
    generate_markdown_docs(args.module_name, output_path)


if __name__ == "__main__":
    main() 