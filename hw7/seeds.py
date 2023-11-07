import random

from faker import Faker
from sqlalchemy.exc import SQLAlchemyError
from faker import Faker
from connect import session
from models import Student, Grade, Subject, Group, Teacher

fake = Faker("uk-UA")

for _ in range(5):
    teacher = Teacher(
        fullname=fake.name()
    )
    session.add(teacher)

for _ in range(3):
    group = Group(
        name=fake.word()
    )
    session.add(group)

for _ in range(30):
    student = Student(
        fullname=fake.name(),
        group_id=random.choice(session.query(Group).all()).id
    )
    session.add(student)

for _ in range(8):
    subjects = Subject(
        name=fake.word(),
        teacher_id=random.choice(session.query(Teacher).all()).id
    )
    session.add(subjects)

for _ in range(30):
    grades = Grade(
        grade=random.randint(1, 12),
        grade_date=fake.date_between(start_date='-5y'),
        student_id=random.choice(session.query(Student).all()).id,
        subjects_id=random.choice(session.query(Subject).all()).id)
    session.add(grades)

session.commit()
