from sqlalchemy import func, desc, select, and_

from models import Grade, Teacher, Student, Group, Subject
from connect import session


def select_01():
    "Знайти 5 студентів із найбільшим середнім балом з усіх предметів"
    """
    SELECT s.fullname, round(avg(g.grade), 2) AS avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 5;
    session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade")).select_from(
        Grade).join(
        Student).group_by(Student.id).order_by(desc("avg_grade")).limit(5).all()
    return result


def select_02():
    "Знайти студента із найвищим середнім балом з певного предмета"
    """
    SELECT g.student_id, ROUND(AVG(g.grade),2) as avgMark FROM grades g
    WHERE g.subject_id = 8
    Group by g.student_id
    ORDER by avgMark DESC
    Limit 1
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label("avgMark")).select_from(
        Grade).join(
        Student).where(Grade.subjects_id == 5).group_by(Student.id).order_by(desc("avgMark")).limit(1).all()
    return result


def select_03():
    "Знайти середній бал у групах з певного предмета"
    """
    SELECT s.group_id, AVG(grade) as average_grade
    FROM students s
    INNER JOIN grades g ON s.id = g.student_id
    WHERE g.subject_id = 5
    GROUP BY s.group_id
    """

    result = session.query(Student.group_id, func.round(func.avg(Grade.grade), 2)).join(Grade,
                                                                                        Student.id == Grade.student_id).filter(
        Grade.subjects_id == 5).group_by(Student.group_id).all()
    return result


def select_04():
    "Знайти середній бал на потоці (по всій таблиці оцінок)"
    """
    SELECT ROUND(AVG(g.grade),2) as avgGrade From grades g
    """
    result = session.query(func.round(func.avg(Grade.grade), 2)).scalar()
    return result


def select_05():
    "Знайти які курси читає певний викладач"
    """
    SELECT t.fullname, s.name FROM teachers t
    JOIN subjects s ON t.id = s.teacher_id
    """
    result = session.query(Teacher.fullname, Subject.name).select_from(Teacher).join(Subject).all()
    return result


def select_06():
    "Знайти список студентів у певній групі"
    """
    SELECT g.name as groupname, COUNT(s.fullname) as quantity From students s
    JOIN groups g ON s.group_id = g.id
    group by g.name
    """
    result = session.query(Group.name, func.count(Student.fullname)).select_from(Group).join(Student).group_by(
        Group.name).all()
    return result


def select_07():
    "Знайти оцінки студентів у окремій групі з певного предмета"
    """
    SELECT s.fullname, g.grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    WHERE s.group_id = 2 AND g.subject_id = 7
    """
    result = session.query(Student.fullname, Grade.grade).select_from(Student).join(Grade).where(
        Student.group_id == 2).where(Grade.subjects_id == 7).all()
    return result


def select_08():
    "Знайти середній бал, який ставить певний викладач зі своїх предметів"
    """
    SELECT s.name, AVG(g.grade) as avrGrade
    FROM subjects s
    LEFT JOIN grades g ON s.id = g.subject_id
    WHERE s.teacher_id = 1
    GROUP BY s.name
    """
    result = session.query(Subject.name, func.round(func.avg(Grade.grade), 2)).select_from(Subject).join(Grade).where(
        Subject.teacher_id == 1).group_by(Subject.name).all()
    return result


def select_09():
    "Знайти список курсів, які відвідує певний студент"
    """
    SELECT groups.name
    FROM students
    INNER JOIN groups ON students.group_id = groups.id
    WHERE students.id = 12;
    """
    result = session.query(Group.name).select_from(Student).join(Group).where(Student.id == 11).scalar()
    return result


def select_10():
    "Список курсів, які певному студенту читає певний викладач"
    """
    SELECT groups.name AS course_name, subjects.name AS subject_name, teachers.name AS teacher_name
    FROM students
    JOIN groups ON students.group_id = groups.id
    JOIN grades ON students.id = grades.student_id
    JOIN subjects ON grades.subject_id = subjects.id
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE students.id = 23
    """

    result = session.query(Group.name,
                           Subject.name,
                           Teacher.fullname).filter(Student.group_id == Group.id,
                                                    Student.id == Grade.student_id,
                                                    Grade.subjects_id == Subject.id,
                                                    Subject.teacher_id == Teacher.id,
                                                    Student.id == 23).all()
    return result


def select_add_01():
    "Середній бал, який певний викладач ставить певному студентові"
    """
    SELECT subjects.teacher_id, AVG(grades.grade) AS average_grade
FROM grades
INNER JOIN subjects ON grades.subject_id = subjects.id
WHERE subjects.teacher_id = 1 AND grades.student_id = 4;
    """

    result = session.query(Teacher.fullname, func.round(func.avg(Grade.grade), 2)).filter(
        Teacher.id == Subject.teacher_id,
        Grade.subjects_id == Subject.id, Subject.teacher_id == 2, Grade.student_id == 4).group_by(
        Teacher.fullname).all()
    return result


def select_add_02():
    "Оцінки студентів у певній групі з певного предмета на останньому занятті"
    """
    SELECT s.fullname, g.grade
    FROM grades g
    INNER JOIN students s ON g.student_id = s.id
    WHERE s.group_id = 1 AND g.subject_id = 4
    ORDER BY g.grade_date DESC
    LIMIT 1;
    
    """
    result = session.query(Student.fullname, Grade.grade).filter(Grade.student_id == Student.id, Student.group_id == 1,
                                                                 Grade.subjects_id == 4).order_by(
        desc(Grade.grade_date)).limit(1).all()
    return result


if __name__ == '__main__':
    print(select_01())
    print("------------------------")
    print(select_02())
    print("------------------------")
    print(select_03())
    print("------------------------")
    print(select_04())
    print("------------------------")
    print(select_05())
    print("------------------------")
    print(select_06())
    print("------------------------")
    print(select_07())
    print("------------------------")
    print(select_08())
    print("------------------------")
    print(select_09())
    print("------------------------")
    print(select_10())
    print("--------Additional tasks----------------")
    print(select_add_01())
    print("------------------------")
    print(select_add_02())
