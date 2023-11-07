import argparse
import datetime
from sqlalchemy import func

from connect import session
from models import Student, Grade, Subject, Group, Teacher

parser = argparse.ArgumentParser(description="CRUD operation for DB")
parser.add_argument("--action", "-a", help="CRUD", required=True)
parser.add_argument("--model", "-m", help="Model", required=True)


parser.add_argument("--fullname", "-fn", help="fullname")
parser.add_argument("--name", "-n", help="name")
parser.add_argument("--grade", "-g", help="grade")
parser.add_argument("--grade_date", "-grdate", help="grade_date")
parser.add_argument("--value", "-v", help="value")
parser.add_argument("--subject_id", "-s", help="subject_id")
parser.add_argument("--id", "-id", help="id")

args = vars(parser.parse_args())
action = args.get("action")
model = args.get("model")

fullname = args.get("fullname")
name = args.get("name")
grade_param = args.get("grade")
grade_date_param = args.get("grade_date")
value = args.get("value")
subject_id_param = args.get("subject_id")
id_param = args.get("id")


models = {
    "Student": Student,
    "Teacher": Teacher,
    "Grade": Grade,
    "Subject": Subject,
    "Group": Group
}


def create():
    if model in models.keys():
        if fullname:
            if model == "Student" and value:
                if 0 < int(value) < (int(session.query(func.count(Group.id)).scalar()) + 1):
                    new_name_plus_group = models.get(model)(fullname=fullname, group_id=int(value))
                    session.add(new_name_plus_group)
                    session.commit()
            if model == "Teacher" and not value:
                new_name = models.get(model)(fullname=fullname)
                session.add(new_name)
                session.commit()

        if name:
            if model == "Subject" and value:
                if 0 < int(value) < (int(session.query(func.count(Teacher.id)).scalar()) + 1):
                    new_group = models.get(model)(name=name, teacher_id=int(value))
                    session.add(new_group)
                    session.commit()
            if not value:
                new_group = models.get(model)(name=name)
                session.add(new_group)
                session.commit()

        if grade_param:
            if grade_date_param:
                date = formatting_data(grade_date_param)
                if date:
                    if value:
                        if int(value) in range(1, int(session.query(func.count(Student.id)).scalar()) + 1):
                            if subject_id_param:
                                if int(subject_id_param) in range(1, int(session.query(
                                        func.count(Subject.id)).scalar()) + 1):
                                    new_grade_plus_date_plus_stid_plus_subid = models.get(model)(grade=int(grade_param),
                                                                                                 grade_date=date,
                                                                                                 student_id=int(value),
                                                                                                 subjects_id=int(
                                                                                                     subject_id_param))
                                    session.add(new_grade_plus_date_plus_stid_plus_subid)
                                    session.commit()
                                else:
                                    print("Current quantity of subject's isn't exist")
                            else:
                                new_grade_plus_date_plus_stid = models.get(model)(grade=int(grade_param),
                                                                                  grade_date=date,
                                                                                  student_id=int(value))
                                session.add(new_grade_plus_date_plus_stid)
                                session.commit()
                        else:
                            print("Current quantity of student's isn't exist")
                    elif not value and not subject_id_param:
                        new_grade_plus_date = models.get(model)(grade=int(grade_param), grade_date=date)
                        session.add(new_grade_plus_date)
                        session.commit()
            elif not grade_param and not value and not subject_id_param:
                new_grade = models.get(model)(grade=int(grade_param))
                session.add(new_grade)
                session.commit()

        if not name and not fullname and not grade_param and not grade_date_param:
            print("Need to define one of columns")
    else:
        print("Current model isn't exist")


def formatting_data(text):
    try:
        form_date = grade_date_param.split(".")
        date_complete = datetime.datetime(year=int(form_date[2]), month=int(form_date[1]),
                                          day=int(form_date[0]))
        return date_complete
    except (IndexError, ValueError):
        raise TypeError("Should to write true date in format 'dd.mm.yyyy'")


def read():
    if model in ["Student", "Teacher"]:
        for number, person in enumerate(session.query(models.get(model)).all()):
            print(number, person.fullname)
    elif model in ["Subject", "Group"]:
        for number, area in enumerate(session.query(models.get(model)).all()):
            print(number, area.name)
    else:
        for person, grade, subject in session.query(Student.fullname, Grade.grade, Subject.name).filter(
                Grade.student_id == Student.id, Grade.subjects_id == Subject.id).all():
            print(person, grade, subject)


def update():
    if model in models.keys():
        if id_param:
            if fullname:
                field = session.query(models.get(model)).filter(models.get(model).id == int(id_param)).one_or_none()
                if field:
                    field.fullname = fullname
                    session.commit()

        else:
            print("Need to identify id")
    else:
        print("Current model isn't exist")

def remove():
    if model in models.keys():
        if id_param:
            if fullname:
                row = session.query(models.get(model)).where(models.get(model).id == int(id_param)).scalar()
                session.delete(row)
                session.commit()
        else:
            print("Need to identify id")
    else:
        print("Current model isn't exist")

crud_operation = {
    "create": create,
    "list": read,
    "update": update,
    "remove": remove
}

if __name__ == '__main__':
    if action in crud_operation.keys():
        crud_operation.get(action)()
