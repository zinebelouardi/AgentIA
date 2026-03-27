import os
import json
import requests
from pathlib import Path
from typing import Dict, List

OLLAMA_MODEL = "qwen2.5:7b" 
OLLAMA_BASE_URL = "http://localhost:11434"
INPUT_DIRECTORY = "./raw_data_extracted_txt"
OUTPUT_DIRECTORY = "./extracted_data_json"

class CosmeticsDataExtractor:
    def __init__(self, model_name: str = OLLAMA_MODEL):
        """Initialize the Ollama-based data extraction pipeline"""
        self.model_name = model_name
        self.api_url = f"{OLLAMA_BASE_URL}/api/generate"
        print(f"Initializing Ollama model: {model_name}")
        self._check_ollama_connection()
        print("Extractor ready!\n")
    
    def _check_ollama_connection(self):
        """Check if Ollama is running and model is available"""
        try:
            response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
            if response.status_code == 200:
                models = response.json().get('models', [])
                model_names = [m.get('name', '') for m in models]
                print(f"✓ Connected to Ollama")
                
                # Check if requested model exists
                model_found = any(self.model_name in name for name in model_names)
                if model_found:
                    print(f"✓ Model '{self.model_name}' is available")
                else:
                    print(f"⚠ Model '{self.model_name}' not found!")
                    print(f"Available models:")
                    for name in model_names:
                        print(f"  - {name}")
                    print(f"\nTo install the model, run: ollama pull {self.model_name}")
            else:
                print("⚠ Ollama connection issue")
        except Exception as e:
            print(f"⚠ Cannot connect to Ollama: {e}")
            print("Make sure Ollama is running in another terminal: 'ollama serve'")
    
    def _create_extraction_prompt(self, text: str) -> str:
        """Create the extraction prompt"""
        
        prompt = f"""You are a data extraction specialist for cosmetics products. Extract structured information from the provided text and return it in JSON format.

DATABASE SCHEMA:
1. products: product_id, category, brand, product_name, price, rank, ingredients_text, combination, dry, normal, oily, sensitive
2. ingredients: ingredient_id, ingredient_name, category, famous_name
3. product_ingredients: id, product_id, ingredient_id
4. chemical_incidents: incident_id, brand, primary_category, sub_category, cas_number, chemical_name, incident_count, initial_date_reported, most_recent_date_reported

EXTRACTION RULES:
- Extract ALL products mentioned in the text
- Identify all ingredients and their properties
- Note skin type suitability (combination, dry, normal, oily, sensitive) as boolean values
- Extract pricing, ranking, and brand information
- Identify any chemical incidents or safety concerns
- Generate unique IDs starting from 1 for each entity type

IMPORTANT - FILLING MISSING DATA:
- For ingredients NOT explicitly detailed in the text, use your knowledge to fill in:
  * category: classify as "emollient", "humectant", "preservative", "surfactant", "thickener", "fragrance", "colorant", "active ingredient", "pH adjuster", "antioxidant", etc.
  * famous_name: provide common/commercial names (e.g., "Vitamin E" for tocopherol, "Vitamin C" for ascorbic acid, "Retinol" for vitamin A)
- For products with incomplete information, infer from context when possible:
  * category: "moisturizer", "cleanser", "serum", "sunscreen", "makeup", "hair care", etc.
  * skin type suitability: use ingredient properties to infer (e.g., hyaluronic acid → all skin types, salicylic acid → oily/acne-prone)
- Only use null when information cannot be extracted OR reasonably inferred from your training knowledge

INPUT TEXT:
{text}

EXAMPLES OF FILLING MISSING DATA:
Example 1 - Ingredient with missing info:
Input: "contains hyaluronic acid"
Output: {{
  "ingredient_name": "hyaluronic acid",
  "category": "humectant",
  "famous_name": "Hyaluronic Acid / HA"
}}

Example 2 - Ingredient with technical name:
Input: "tocopherol acetate is added"
Output: {{
  "ingredient_name": "tocopherol acetate",
  "category": "antioxidant",
  "famous_name": "Vitamin E"
}}

Example 3 - Product with missing skin type info:
Input: "moisturizing cream with ceramides and niacinamide"
Output: {{
  "combination": true,
  "dry": true,
  "normal": true,
  "oily": true,
  "sensitive": true
}}
(ceramides and niacinamide are suitable for all skin types)

OUTPUT FORMAT (strict JSON):
{{
  "products": [
    {{
      "product_id": 1,
      "category": "string",
      "brand": "string",
      "product_name": "string",
      "price": "string or null",
      "rank": "integer or null",
      "ingredients_text": "full ingredients list",
      "combination": true/false,
      "dry": true/false,
      "normal": true/false,
      "oily": true/false,
      "sensitive": true/false
    }}
  ],
  "ingredients": [
    {{
      "ingredient_id": 1,
      "ingredient_name": "string",
      "category": "string or null",
      "famous_name": "string or null"
    }}
  ],
  "product_ingredients": [
    {{
      "id": 1,
      "product_id": 1,
      "ingredient_id": 1
    }}
  ],
  "chemical_incidents": [
    {{
      "incident_id": 1,
      "brand": "string",
      "primary_category": "string",
      "sub_category": "string or null",
      "cas_number": "string or null",
      "chemical_name": "string",
      "incident_count": integer,
      "initial_date_reported": "date or null",
      "most_recent_date_reported": "date or null"
    }}
  ]
}}

Return ONLY the JSON object, no additional text or markdown formatting."""
        
        return prompt
    
    def _split_text_into_chunks(self, text: str, chunk_size: int = 6000, overlap: int = 500) -> List[str]:
        """Split text into overlapping chunks"""
        chunks = []
        start = 0
        text_length = len(text)
        
        while start < text_length:
            end = start + chunk_size
            chunk = text[start:end]
            chunks.append(chunk)
            start += (chunk_size - overlap)  # Move forward with overlap
        
        return chunks
    
    def _merge_extracted_data(self, all_chunks_data: List[Dict]) -> Dict:
        """Merge data from multiple chunks, removing duplicates"""
        merged = {
            "products": [],
            "ingredients": [],
            "product_ingredients": [],
            "chemical_incidents": []
        }
        
        # Track seen items to avoid duplicates
        seen_products = set()
        seen_ingredients = {}  # name -> ingredient data
        seen_product_ings = set()
        seen_incidents = set()
        
        for chunk_data in all_chunks_data:
            # Merge products
            for product in chunk_data.get("products", []):
                product_key = (
                    product.get("brand", ""),
                    product.get("product_name", "")
                )
                if product_key not in seen_products and product.get("product_name"):
                    seen_products.add(product_key)
                    product["product_id"] = len(merged["products"]) + 1
                    merged["products"].append(product)
            
            # Merge ingredients (by name, keeping most complete data)
            for ingredient in chunk_data.get("ingredients", []):
                ing_name = ingredient.get("ingredient_name", "").lower().strip()
                if ing_name:
                    if ing_name not in seen_ingredients:
                        seen_ingredients[ing_name] = ingredient
                    else:
                        # Keep the version with more non-null fields
                        existing = seen_ingredients[ing_name]
                        new_non_null = sum(1 for v in ingredient.values() if v is not None)
                        existing_non_null = sum(1 for v in existing.values() if v is not None)
                        if new_non_null > existing_non_null:
                            seen_ingredients[ing_name] = ingredient
            
            # Merge chemical incidents
            for incident in chunk_data.get("chemical_incidents", []):
                incident_key = (
                    incident.get("brand", ""),
                    incident.get("chemical_name", ""),
                    incident.get("cas_number", "")
                )
                if incident_key not in seen_incidents and incident.get("chemical_name"):
                    seen_incidents.add(incident_key)
                    incident["incident_id"] = len(merged["chemical_incidents"]) + 1
                    merged["chemical_incidents"].append(incident)
        
        # Add ingredients with new IDs
        for idx, ingredient in enumerate(seen_ingredients.values(), 1):
            ingredient["ingredient_id"] = idx
            merged["ingredients"].append(ingredient)
        
        # Rebuild product_ingredients with correct IDs
        # This is approximate - we'll link based on matching names
        for product_idx, product in enumerate(merged["products"], 1):
            ingredients_text = product.get("ingredients_text", "")
            if ingredients_text:
                for ingredient in merged["ingredients"]:
                    ing_name = ingredient.get("ingredient_name", "").lower()
                    if ing_name and ing_name in ingredients_text.lower():
                        merged["product_ingredients"].append({
                            "id": len(merged["product_ingredients"]) + 1,
                            "product_id": product_idx,
                            "ingredient_id": ingredient["ingredient_id"]
                        })
        
        return merged
    
    def extract_from_text(self, text: str) -> Dict:
        """Extract structured data from text using Ollama API with sliding window"""
        # Split into chunks if text is too long
        chunk_size = 6000  # characters per chunk
        overlap = 500      # overlap between chunks
        
        if len(text) <= chunk_size:
            # Process as single chunk
            return self._extract_single_chunk(text)
        else:
            # Process with sliding window
            chunks = self._split_text_into_chunks(text, chunk_size, overlap)
            print(f"  → Split into {len(chunks)} chunks")
            
            all_chunks_data = []
            for i, chunk in enumerate(chunks, 1):
                print(f"  → Processing chunk {i}/{len(chunks)}...", end=" ", flush=True)
                chunk_data = self._extract_single_chunk(chunk)
                all_chunks_data.append(chunk_data)
                print("Done")
            
            # Merge results from all chunks
            print(f"  → Merging results...", end=" ", flush=True)
            merged_data = self._merge_extracted_data(all_chunks_data)
            print("Done")
            
            return merged_data
    
    def _extract_single_chunk(self, text: str) -> Dict:
        """Extract data from a single text chunk"""
        try:
            prompt = self._create_extraction_prompt(text)
            
            payload = {
                "model": self.model_name,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "options": {
                    "temperature": 0.1,
                    "num_ctx": 4096,
                    "top_p": 0.9
                }
            }
            
            response = requests.post(self.api_url, json=payload, timeout=600)
            
            if response.status_code != 200:
                error_msg = response.json().get('error', 'Unknown error')
                print(f"Failed - {error_msg}")
                return self._get_empty_schema()
            
            result = response.json()
            response_text = result.get("response", "{}")
            
            # Clean up response
            response_text = response_text.strip()
            if response_text.startswith("```json"):
                response_text = response_text.replace("```json", "").replace("```", "").strip()
            elif response_text.startswith("```"):
                response_text = response_text.replace("```", "").strip()
            
            extracted_data = json.loads(response_text)
            return extracted_data
            
        except json.JSONDecodeError as e:
            print(f"JSON parse error")
            return self._get_empty_schema()
        except requests.Timeout:
            print(f"Timeout")
            return self._get_empty_schema()
        except requests.RequestException as e:
            print(f"Request error")
            return self._get_empty_schema()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"Error: {e}")
            return self._get_empty_schema()
    
    def _get_empty_schema(self) -> Dict:
        """Return empty schema structure"""
        return {
            "products": [],
            "ingredients": [],
            "product_ingredients": [],
            "chemical_incidents": []
        }
    
    def process_file(self, file_path: str) -> Dict:
        """Process a single text file"""
        file_name = Path(file_path).name
        print(f"Processing: {file_name}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception as e:
            print(f"  ✗ Error reading file: {e}")
            return self._get_empty_schema()
        
        if not text.strip():
            print(f"  ⚠ Empty file, skipping")
            return self._get_empty_schema()
        
        # Show file size
        file_size = len(text)
        print(f"  File size: {file_size:,} characters")
        
        # Process with sliding window (no truncation!)
        extracted = self.extract_from_text(text)
        
        # Check if extraction was successful
        num_products = len(extracted.get('products', []))
        num_ingredients = len(extracted.get('ingredients', []))
        num_incidents = len(extracted.get('chemical_incidents', []))
        
        if num_products > 0 or num_ingredients > 0 or num_incidents > 0:
            print(f"  ✓ Extracted: {num_products} products, "
                  f"{num_ingredients} ingredients, {num_incidents} incidents")
        else:
            print(f"  ⚠ No data extracted")
        
        return extracted
    
    def process_directory(self, input_dir: str, output_dir: str):
        """Process all text files in a directory"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        txt_files = list(input_path.glob("*.txt"))
        
        if not txt_files:
            print(f"No .txt files found in {input_dir}")
            print(f"Please add .txt files to: {input_path.absolute()}")
            return
        
        print(f"Found {len(txt_files)} text files\n")
        
        all_results = []
        successful = 0
        failed = 0
        
        try:
            for i, txt_file in enumerate(txt_files, 1):
                print(f"\n[{i}/{len(txt_files)}] ", end="")
                result = self.process_file(str(txt_file))
                
                # Check if extraction was successful
                has_data = (len(result.get('products', [])) > 0 or 
                           len(result.get('ingredients', [])) > 0 or
                           len(result.get('chemical_incidents', [])) > 0)
                
                if has_data:
                    successful += 1
                else:
                    failed += 1
                
                # Save individual result
                output_file = output_path / f"{txt_file.stem}_extracted.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                
                all_results.append({
                    "source_file": txt_file.name,
                    "data": result
                })
                
                print(f"  → Saved to: {output_file.name}")
                
        except KeyboardInterrupt:
            print("\n\n⚠ Process interrupted by user")
            print(f"Processed {i} out of {len(txt_files)} files before interruption")
        
        # Save combined results (even if interrupted)
        if all_results:
            combined_file = output_path / "all_extracted_data.json"
            with open(combined_file, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            
            print(f"\n✓ Results saved to: {combined_file}")
            print(f"✓ Successful extractions: {successful}/{len(all_results)}")
            if failed > 0:
                print(f"⚠ Failed/empty extractions: {failed}/{len(all_results)}")
            self._print_summary(all_results)
    
    def _print_summary(self, results: List[Dict]):
        """Print extraction summary"""
        total_products = sum(len(r["data"].get("products", [])) for r in results)
        total_ingredients = sum(len(r["data"].get("ingredients", [])) for r in results)
        total_product_ings = sum(len(r["data"].get("product_ingredients", [])) for r in results)
        total_incidents = sum(len(r["data"].get("chemical_incidents", [])) for r in results)
        
        print("\n" + "=" * 60)
        print("EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Files processed: {len(results)}")
        print(f"Total products: {total_products}")
        print(f"Total unique ingredients: {total_ingredients}")
        print(f"Total product-ingredient links: {total_product_ings}")
        print(f"Total chemical incidents: {total_incidents}")
        print("=" * 60)


def main():
    """Main execution function"""
    
    print("=" * 60)
    print("Cosmetics Data Extraction Pipeline (Ollama)")
    print("=" * 60 + "\n")
    
    # Check if input directory exists
    if not os.path.exists(INPUT_DIRECTORY):
        os.makedirs(INPUT_DIRECTORY)
        print(f"Created input directory: {INPUT_DIRECTORY}")
        print(f"Please add your .txt files to: {Path(INPUT_DIRECTORY).absolute()}")
        print("Then run the script again.\n")
        return
    
    # Initialize extractor
    try:
        extractor = CosmeticsDataExtractor(model_name=OLLAMA_MODEL)
        
        # Process all files
        extractor.process_directory(INPUT_DIRECTORY, OUTPUT_DIRECTORY)
        
        print("\n✓ Extraction complete!")
        
    except KeyboardInterrupt:
        print("\n\n✓ Extraction stopped by user")
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()