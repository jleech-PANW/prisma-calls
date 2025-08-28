import os
import argparse
import sys

def create_certificate_bundle(cert_directory, output_file="certificate-bundle.pem"):
    """
    Finds all certificate files (.crt, .cer, .pem) in a given directory,
    reads them, and concatenates them into a single PEM bundle file.

    This script ensures that each certificate is separated by a newline
    for correct formatting.

    Args:
        cert_directory (str): The path to the directory containing certificate files.
        output_file (str): The name of the output bundle file to be created.
    """
    # Define the file extensions to look for
    valid_extensions = ('.crt', '.cer', '.pem')
    
    # --- 1. Validate Input Directory ---
    if not os.path.isdir(cert_directory):
        print(f"Error: The specified directory does not exist: '{cert_directory}'", file=sys.stderr)
        sys.exit(1)

    # --- 2. Find Certificate Files ---
    try:
        all_files = os.listdir(cert_directory)
        cert_files = [f for f in all_files if f.lower().endswith(valid_extensions)]
    except Exception as e:
        print(f"Error: Could not read directory '{cert_directory}'. Reason: {e}", file=sys.stderr)
        sys.exit(1)

    if not cert_files:
        print(f"Warning: No certificate files with extensions {valid_extensions} found in '{cert_directory}'.")
        return

    print(f"Found {len(cert_files)} certificate files to combine: {', '.join(cert_files)}")

    # --- 3. Combine Certificates into a Single String ---
    bundle_content = []
    for cert_file in cert_files:
        cert_path = os.path.join(cert_directory, cert_file)
        try:
            with open(cert_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                # Ensure the content looks like a PEM block
                if content.startswith('-----BEGIN CERTIFICATE-----') and content.endswith('-----END CERTIFICATE-----'):
                    bundle_content.append(content)
                    print(f"  -> Added {cert_file}")
                else:
                    print(f"  -> Skipped {cert_file} (does not appear to be a valid PEM certificate)")
        except Exception as e:
            print(f"  -> Error reading {cert_file}. Reason: {e}", file=sys.stderr)

    # --- 4. Write the Combined Content to the Output File ---
    if not bundle_content:
        print("No valid certificates were found to create a bundle.", file=sys.stderr)
        sys.exit(1)
        
    # Join all individual certificate blocks with a newline
    final_bundle = "\n".join(bundle_content) + "\n" # Add a final newline for POSIX compliance
    
    output_path = os.path.join(cert_directory, output_file)
    try:
        with open(output_path, 'w', encoding='utf-8') as bundle_file:
            bundle_file.write(final_bundle)
        print(f"\nSuccess! Bundle created at: '{output_path}'")
    except Exception as e:
        print(f"\nError: Could not write to output file '{output_path}'. Reason: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    # --- Command-Line Interface Setup ---
    parser = argparse.ArgumentParser(
        description="A script to combine multiple certificate files (.crt, .cer, .pem) from a directory into a single bundle file.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "directory",
        type=str,
        help="Required: The path to the directory containing the certificate files."
    )
    
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="certificate-bundle.pem",
        help="Optional: The name for the final output bundle file (default: certificate-bundle.pem)."
    )
    
    args = parser.parse_args()
    
    create_certificate_bundle(args.directory, args.output)
