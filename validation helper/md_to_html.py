import os
import markdown
import json
from pathlib import Path
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name, JsonLexer
from markdown.extensions.codehilite import CodeHiliteExtension
from markdown.extensions.fenced_code import FencedCodeExtension

def convert_md_to_html(input_folder):
    # Create formatter for Pygments with light theme
    formatter = HtmlFormatter(style='default')
    pygments_css = formatter.get_style_defs('.codehilite')
    
    # Create markdown converter instance with syntax highlighting
    md = markdown.Markdown(extensions=[
        'tables',
        'toc',
        CodeHiliteExtension(css_class='codehilite'),
        FencedCodeExtension()
    ])
    
    # Create html output folder if it doesn't exist
    html_folder = os.path.join(input_folder, 'html')
    os.makedirs(html_folder, exist_ok=True)
    
    # HTML template with light theme
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
        <style>
            body {{
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                line-height: 1.6;
                background-color: #ffffff;
                color: #24292e;
            }}
            
            /* Code styling */
            code {{
                background-color: #f6f8fa;
                padding: 2px 5px;
                border-radius: 3px;
                font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
                font-size: 85%;
                color: #24292e;
            }}
            
            pre {{
                background-color: #f6f8fa;
                padding: 16px;
                border-radius: 6px;
                overflow-x: auto;
                margin: 20px 0;
                border: 1px solid #e1e4e8;
            }}
            
            /* Headers */
            h1, h2, h3, h4, h5, h6 {{
                color: #24292e;
                font-weight: 600;
                line-height: 1.25;
                margin-top: 24px;
                margin-bottom: 16px;
            }}
            
            h1 {{
                font-size: 2em;
                padding-bottom: 0.3em;
                border-bottom: 1px solid #eaecef;
            }}
            
            h2 {{
                font-size: 1.5em;
                padding-bottom: 0.3em;
                border-bottom: 1px solid #eaecef;
            }}
            
            /* Links */
            a {{
                color: #0366d6;
                text-decoration: none;
            }}
            
            a:hover {{
                text-decoration: underline;
            }}
            
            /* Tables */
            table {{
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
            }}
            
            th, td {{
                border: 1px solid #e1e4e8;
                padding: 6px 13px;
            }}
            
            th {{
                background-color: #f6f8fa;
                font-weight: 600;
            }}
            
            tr:nth-child(2n) {{
                background-color: #f6f8fa;
            }}
            
            /* Lists */
            ul, ol {{
                padding-left: 2em;
            }}
            
            li {{
                margin: 0.25em 0;
            }}
            
            /* Blockquotes */
            blockquote {{
                margin: 0;
                padding: 0 1em;
                color: #6a737d;
                border-left: 0.25em solid #dfe2e5;
            }}
            
            /* Horizontal rule */
            hr {{
                height: 0.25em;
                padding: 0;
                margin: 24px 0;
                background-color: #e1e4e8;
                border: 0;
            }}
            
            {pygments_css}
            
            /* Override any dark background from pygments for light theme */
            .codehilite {{
                background-color: #f6f8fa !important;
            }}
        </style>
    </head>
    <body>
        {content}
    </body>
    </html>
    """
    
    def process_code_blocks(html_content):
        """
        Post-process HTML to add additional syntax highlighting for specific languages
        """
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all code blocks
        for pre in soup.find_all('pre'):
            code = pre.find('code')
            if code and 'class' in code.attrs:
                classes = code['class']
                language = None
                
                # Extract language from class
                for cls in classes:
                    if cls.startswith('language-'):
                        language = cls.replace('language-', '')
                        break
                
                if language:
                    try:
                        if language.lower() == 'json':
                            lexer = JsonLexer()
                        else:
                            lexer = get_lexer_by_name(language)
                            
                        code_text = code.string if code.string else code.text
                        highlighted = highlight(code_text, lexer, formatter)
                        new_pre = BeautifulSoup(highlighted, 'html.parser')
                        pre.replace_with(new_pre)
                    except:
                        # If lexer not found, leave as-is
                        pass
        
        return str(soup)
    
    # Get all md files in the input folder
    md_files = Path(input_folder).glob('*.md')
    
    for md_file in md_files:
        # Read markdown content
        with open(md_file, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        # Convert to HTML
        html_content = md.convert(md_content)
        
        # Process code blocks for additional syntax highlighting
        html_content = process_code_blocks(html_content)
        
        # Create complete HTML document
        title = md_file.stem.replace('_', ' ').title()
        final_html = html_template.format(
            title=title,
            content=html_content,
            pygments_css=pygments_css
        )
        
        # Generate output filename
        output_file = os.path.join(html_folder, f"{md_file.stem}.html")
        
        # Save HTML file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(final_html)
        
        # Reset markdown converter for next file
        md.reset()
        
        print(f"Converted {md_file.name} to {os.path.basename(output_file)}")

if __name__ == "__main__":
    # Get current directory as default
    current_dir = "reports"
    convert_md_to_html(current_dir)