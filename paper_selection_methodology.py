"""
paper_selection_methodology.py

Methodology for selecting papers from syllabi extractions to reduce from ~1000 to 100-200.

Criteria:
1. Popularity/frequency across syllabi
2. Supplement with new works that seem strong (recency)
3. Influence (citations on Google Scholar)
4. Diversity: regional base of author (Western vs non-Western)
5. Diversity: school of thought (microeconomic, institutional, social cohesion/bottom-up)
6. Manual rating from expert recommendation

Uses LLM (Claude) for classification and citation estimation where needed.
"""

import json
import math
import os
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
from dotenv import load_dotenv
import anthropic
from scholarly import scholarly

# Load environment
load_dotenv()
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

CURRENT_YEAR = 2026

def load_aggregated_papers() -> Dict[str, Dict]:
    """Load the aggregated papers data."""
    with open('aggregated_papers.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def compute_frequency_score(count: int, max_count: int) -> float:
    """Normalize frequency score."""
    return count / max_count if max_count > 0 else 0

def compute_recency_score(year: int) -> float:
    """Compute recency score using exponential decay from current year."""
    if not year or year > CURRENT_YEAR:
        return 0
    # Exponential decay: higher for newer papers
    decay_rate = 0.3  # Increased for stronger recency preference
    years_old = CURRENT_YEAR - year
    return math.exp(-decay_rate * years_old)

def get_citation_count(title: str, authors: str) -> int:
    """Get citation count from Google Scholar using scholarly."""
    try:
        # Clean title and authors
        clean_title = title.strip().lower()
        first_author = authors.split(';')[0].split(',')[0].strip().lower() if authors else ""
        query = f"{clean_title} {first_author}"
        search_results = scholarly.search_pubs(query)
        pub = next(search_results, None)
        if pub and 'num_citations' in pub:
            return pub['num_citations']
        return 0
    except Exception as e:
        print(f"Error fetching citations for '{title}': {e}")
        return 0

def estimate_citation_score(citations: int) -> float:
    """Normalize citation score. Assume max ~1000 citations."""
    max_citations = 1000
    return min(citations / max_citations, 1.0)

def classify_regional(authors: str) -> float:
    """Use LLM to classify authors as Western (0) or non-Western (1)."""
    if not authors or not ANTHROPIC_API_KEY:
        return 0.5  # Neutral if no data
    prompt = f"Classify the primary regional background of the authors '{authors}' in political economy/development research. Return 0 for primarily Western/European/North American authors, 1 for primarily non-Western (Asia, Africa, Latin America, etc.), 0.5 for mixed or unknown. Respond with only the number."
    try:
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=5,
            messages=[{"role": "user", "content": prompt}]
        )
        score = float(response.content[0].text.strip())
        return min(max(score, 0), 1)
    except:
        return 0.5

def classify_school_of_thought(title: str, authors: str) -> float:
    """Use LLM to classify school of thought."""
    if not ANTHROPIC_API_KEY:
        return 0.5
    prompt = f"Classify this paper into a school of thought in political economy/development: '{title}' by {authors}. Return 0 for microeconomic/foundational/economic theory, 0.5 for institutional/political economy, 1 for social cohesion/bottom-up/community-based approaches. Respond with only the number."
    try:
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=5,
            messages=[{"role": "user", "content": prompt}]
        )
        score = float(response.content[0].text.strip())
        return min(max(score, 0), 1)
    except:
        return 0.5

def compute_manual_score() -> float:
    """Placeholder for manual expert rating. Set to 0 for now."""
    return 0.0

def compute_final_score(freq: float, recency: float, citation: float, regional: float, school: float, manual: float) -> float:
    """Compute weighted final score."""
    weights = {
        'frequency': 0.25,  # Reduced to balance with citations
        'recency': 0.25,    # Increased for newer works
        'citation': 0.30,   # Increased for influence
        'regional': 0.10,   # Maintained
        'school': 0.10,     # Maintained
        'manual': 0.0       # Removed since unimplemented
    }
    return (freq * weights['frequency'] +
            recency * weights['recency'] +
            citation * weights['citation'] +
            regional * weights['regional'] +
            school * weights['school'] +
            manual * weights['manual'])

def main():
    papers = load_aggregated_papers()
    
    # Find max frequency
    max_count = max(p['count'] for p in papers.values())
    
    scored_papers = {}
    
    print(f"Processing {len(papers)} papers...")
    
    for i, (title_lower, data) in enumerate(papers.items()):
        details = data['details']
        if not details:
            continue
        
        year = int(details.get('year', 0)) if details.get('year') else 0
        authors = details.get('authors', '')
        
        # Compute scores
        freq_score = compute_frequency_score(data['count'], max_count)
        recency_score = compute_recency_score(year)
        
        # Citations - fetch for all, but with delay to avoid rate limits
        import time
        citations = get_citation_count(details.get('title', ''), authors)
        time.sleep(0.5)  # Delay to be respectful
        citation_score = estimate_citation_score(citations)
        
        regional_score = classify_regional(authors)
        time.sleep(0.1)
        school_score = classify_school_of_thought(details.get('title', ''), authors)
        time.sleep(0.1)
        manual_score = compute_manual_score()
        
        final_score = compute_final_score(freq_score, recency_score, citation_score, regional_score, school_score, manual_score)
        
        scored_papers[title_lower] = {
            'title': details.get('title', ''),
            'authors': authors,
            'year': year,
            'frequency_score': freq_score,
            'recency_score': recency_score,
            'citation_score': citation_score,
            'citations': citations,
            'regional_score': regional_score,
            'school_score': school_score,
            'manual_score': manual_score,
            'final_score': final_score,
            'count': data['count']
        }
        
        if (i+1) % 100 == 0:
            print(f"Processed {i+1} papers...")
    
    # Save scores
    with open('paper_selection_scores_new.json', 'w', encoding='utf-8') as f:
        json.dump(scored_papers, f, indent=2, ensure_ascii=False)
    
    # Sort by final score descending
    sorted_papers = sorted(scored_papers.items(), key=lambda x: x[1]['final_score'], reverse=True)
    
    # Select top 200
    selected = sorted_papers[:200]
    
    # Save selected to CSV
    rows = []
    for title_lower, scores in selected:
        row = {
            'Title': scores['title'],
            'Authors': scores['authors'],
            'Year': scores['year'],
            'Frequency Score': scores['frequency_score'],
            'Recency Score': scores['recency_score'],
            'Citation Score': scores['citation_score'],
            'Citations': scores['citations'],
            'Regional Score': scores['regional_score'],
            'School Score': scores['school_score'],
            'Manual Score': scores['manual_score'],
            'Final Score': scores['final_score'],
            'Syllabi Count': scores['count']
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df.to_csv('selected_papers_top200.csv', index=False)
    
    print(f"Processed {len(scored_papers)} papers.")
    print("Top 10 selected papers:")
    for i, (title, scores) in enumerate(selected[:10]):
        print(f"{i+1}. {scores['title']} (Score: {scores['final_score']:.3f})")

if __name__ == "__main__":
    main()