import pandas as pd
import os

main_dir = r'C:\Users\AshwinR\Documents\project\dataengineering\DATA_MART\data'
dim_course = pd.read_csv(os.path.join(main_dir, 'mart_DimCourse.csv'))
dim_department = pd.read_csv(os.path.join(main_dir, 'mart_DimDepartment.csv'))
dim_skill = pd.read_csv(os.path.join(main_dir, 'mart_DimSkill.csv'))
dim_user = pd.read_csv(os.path.join(main_dir, 'mart_DimUser.csv'))
fact_course_user = pd.read_csv(os.path.join(main_dir, 'mart_FactCourseUser.csv'))
fact_skill_user = pd.read_csv(os.path.join(main_dir, 'mart_FactSkillUser.csv'))
competency_mapping = {
    'beginner': 1,
    'intermediate': 2,
    'advanced': 3
}

# Map competency values in fact_skill_user
fact_skill_user['competency_numeric'] = fact_skill_user['competency'].map(competency_mapping)

# 1. User-Course Participation Summary
# Tracks the number of courses a user has taken and their average score
user_course_summary = fact_course_user.groupby('user_id').agg(
    courses_taken=('course_id', 'count'),
    avg_score=('user_score', 'mean')
).reset_index()
user_course_summary.to_csv('user_course_summary.csv', index=False)

# 2. Department-Course Enrollment
# Shows how many users from each department are enrolled in courses and their average scores
department_course_enrollment = fact_course_user.groupby('department_name').agg(
    users_enrolled=('user_id', 'nunique'),
    avg_score=('user_score', 'mean')
).reset_index()
department_course_enrollment.to_csv('department_course_enrollment.csv', index=False)

# 3. Course Popularity
# Ranks courses by the number of users enrolled
course_popularity = fact_course_user.groupby(['course_id', 'course_name']).agg(
    total_users=('user_id', 'count')
).reset_index().sort_values(by='total_users', ascending=False)
course_popularity.to_csv('course_popularity.csv', index=False)

# 4. Skill Distribution Across Users
# Provides a breakdown of how many users possess each skill and their average competency level
skill_distribution = fact_skill_user.groupby(['skill_id', 'skill_name']).agg(
    users_with_skill=('user_id', 'nunique'),
    avg_competency=('competency_numeric', 'mean')  # Use the mapped numeric values
).reset_index()
skill_distribution.to_csv('skill_distribution.csv', index=False)

# 5. Top Performing Users
# Highlights users with the highest average scores in courses
top_users = fact_course_user.groupby('user_id').agg(
    avg_score=('user_score', 'mean')
).reset_index().sort_values(by='avg_score', ascending=False).head(10)
top_users.to_csv('top_users.csv', index=False)

# 6. User Skill Levels by Department
# Displays average competency per department for each skill
department_skill_levels = fact_skill_user.groupby(['department_name', 'skill_name']).agg(
    avg_competency=('competency_numeric', 'mean')  # Use the mapped numeric values
).reset_index()
department_skill_levels.to_csv('department_skill_levels.csv', index=False)

# 7. Course Completion Rates by Department
# Measures how many courses each department completes and their average scores
department_course_completion = fact_course_user.groupby('department_name').agg(
    total_courses_completed=('course_id', 'count'),
    avg_score=('user_score', 'mean')
).reset_index()
department_course_completion.to_csv('department_course_completion.csv', index=False)

# 8. Courses Created by Department
# Lists courses and their creators by department
courses_by_department = fact_course_user.groupby(['department_name', 'course_name']).agg(
    courses_created=('course_id', 'count')
).reset_index()
courses_by_department.to_csv('courses_by_department.csv', index=False)

# 9. Skill Competency Growth
# Tracks change in competency levels over time for users
skill_growth = fact_skill_user.groupby(['user_id', 'skill_name']).agg(
    first_competency=('competency_numeric', 'first'),  # Use the mapped numeric values
    last_competency=('competency_numeric', 'last')  # Use the mapped numeric values
).reset_index()
skill_growth.to_csv('skill_growth.csv', index=False)

# 10. User Engagement Score
# Combines course completion and skill competency to rate user engagement
user_engagement = fact_course_user.groupby('user_id').agg(
    avg_course_score=('user_score', 'mean'),
    total_courses=('course_id', 'count')
).merge(
    fact_skill_user.groupby('user_id').agg(avg_skill_competency=('competency_numeric', 'mean')),  # Use the mapped numeric values
    on='user_id'
).reset_index()
user_engagement.to_csv('user_engagement.csv', index=False)