from datasync.utils import MysqlReader, Neo4jWriter


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    # 1. 分类
    def sync_category(self):
        sql = """
        SELECT id, category_name AS name
        FROM base_category_info
        WHERE deleted = '0'
        """
        data = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Category", data)

    # 2. 学科
    def sync_subject(self):
        sql = """
        SELECT id, subject_name AS name, category_id
        FROM base_subject_info
        WHERE deleted = '0'
        """
        data = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Subject", data)

        # 建立关系：学科 -> 分类
        relations = [{"start_id": row["id"], "end_id": row["category_id"]}
                     for row in data if row.get("category_id")]
        self.neo4j_writer.write_relations("Belong", "Subject", "Category", relations)

    # 3. 课程
    def sync_course(self):
        sql = """
        SELECT id, course_name AS name, subject_id
        FROM course_info
        WHERE deleted = '0'
        """
        data = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Course", data)

        # 建立关系：课程 -> 学科
        relations = [{"start_id": row["id"], "end_id": row["subject_id"]}
                     for row in data if row.get("subject_id")]
        self.neo4j_writer.write_relations("Belong", "Course", "Subject", relations)

    # 6. 章节
    def sync_chapter(self):
        sql = """
        SELECT id, chapter_name AS name, course_id
        FROM chapter_info
        WHERE deleted = '0'
        """
        data = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Chapter", data)

        # 建立关系：章节 -> 课程
        relations = [{"start_id": row["id"], "end_id": row["course_id"]}
                     for row in data if row.get("course_id")]
        self.neo4j_writer.write_relations("Belong", "Chapter", "Course", relations)

    # 7. 视频
    def sync_video(self):
        sql = """
        SELECT id, video_name AS name, chapter_id
        FROM video_info
        WHERE deleted = '0'
        """
        data = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Video", data)

        # 建立关系：视频 -> 章节
        relations = [{"start_id": row["id"], "end_id": row["chapter_id"]}
                     for row in data if row.get("chapter_id")]
        self.neo4j_writer.write_relations("Belong", "Video", "Chapter", relations)

    # 8. 试卷
    def sync_paper(self):
        sql = """
        SELECT id, paper_title AS name, course_id
        FROM test_paper
        WHERE deleted = '0'
        """
        data = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Paper", data)

        # 建立关系：试卷 -> 课程
        relations = [{"start_id": row["id"], "end_id": row["course_id"]}
                     for row in data if row.get("course_id")]
        self.neo4j_writer.write_relations("Belong", "Paper", "Course", relations)

    # 9. 试题
    def sync_question(self):
        sql = """
        select t1.id,t1.name,t2.paper_id from
        (
        SELECT id, question_txt AS name
        FROM test_question_info
        WHERE deleted = '0'
        ) t1 join test_paper_question t2 on t1.id = t2.question_id
        """
        data = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Question", data)

        # 建立关系：试题 -> 试卷
        relations = [{"start_id": row["id"], "end_id": row["paper_id"]}
                     for row in data if row.get("paper_id")]
        self.neo4j_writer.write_relations("Belong", "Question", "Paper", relations)


if __name__ == '__main__':
    sync = TableSynchronizer()
    sync.sync_category()
    sync.sync_subject()
    sync.sync_course()
    sync.sync_chapter()
    sync.sync_video()
    sync.sync_paper()
    sync.sync_question()
