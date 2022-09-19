import os
import xlrd
import json

from multiprocessing import Pool
import ru_core_news_lg


def get_postupi_online_professions() -> set:
    book = xlrd.open_workbook("profession_postupi_online.xlsx")
    sheet = book.sheet_by_index(0)
    return set(sheet.col_values(2)[1:])


def get_edwica_professions() -> set:
    path = "Professions"
    values = set()

    for ex_file in os.listdir(path):
        if not ex_file.endswith(".xlsx"): continue
        book = xlrd.open_workbook(os.path.join(path, ex_file))
        sheet = book.sheet_by_name("Вариации названий")
    
        values |= set(sheet.col_values(6)[1:])
    
    return values

def find_skills_like_professions(professions_set: set): 
    """Будем брать по 10 профессий и кидать их в пул многопотока"""
    start = 0 
    for stop in range(0, len(professions_set)+1, 10):
        professions = tuple(professions_set)[start:stop]
        start = stop
        with Pool(10) as process:
            process.map_async(
                func=check_similar_between_professions_and_skills,
                iterable=professions, 
                error_callback=lambda x: exit(x))
            process.close()
            process.join()

def check_similar_between_professions_and_skills(profession: str) -> set:
    """Вернет множество скиллов, похожих на название профессий"""
    nlp = ru_core_news_lg.load()
    if not profession: return 
    doc1 = nlp(text=profession)
    for skill in skills:
        if not skill: continue 
        doc2 = nlp(text=skill)
        similarity = doc1.similarity(doc2)
        if similarity >= 50:
            exit(f"{profession=}\t{skill=}\t{similarity=}")


if __name__ == "__main__":
    edwica_professions = get_edwica_professions()
    postupi_professions = get_postupi_online_professions()
    skills = set([item['demand_name'] for item in json.load(open("skiils.json", "r"))])

    find_skills_like_professions(professions_set=edwica_professions | postupi_professions)