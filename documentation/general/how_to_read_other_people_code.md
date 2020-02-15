# Refs

// https://spin.atomicobject.com/2017/06/01/how-to-read-code/
// https://www.joelonsoftware.com/2000/04/06/things-you-should-never-do-part-i/

## Reading other people code
- People don't like reading other people's code
  - It needs to be done: nobody can code in a vacuum

- When done with right attitude it can be very enjoyable and you can learn

## Rewrite coding

- "This code is a mess. It needs to be re-written"
  - The answer is: ABSOLUTELY NO!

- The best case of a code rewrite is to have:
  - the same code (in reality is likely that new bugs entered the system)
  - with a different complexity that you only now understand
  - after a long time and effort

## The right attitude
- Assume that whoever wrote the code knew what he / she was doing
  - If that's not the case he / she would have already been fired from the team
  - Therefore he / she is as competent than you
- No, you are not the best coder on the planet
- "Why is the code so complicated? I would have done XYZ and make it much
  simpler?"
  - No, you can't write the code in a simpler way
  - The complexity is almost always needed to solve the complex problem we have
  - Be humble

## Reading other people code is painful
- When you write code you think about it and you build your mental model
  - You see the problems, understand them, and then appreciate why a complex
    solution is needed
  - Instead of thinking "why so complicated? I would have done in 5 mins much
    better"

- The problem is that code reflects the thought process of the person who wrote
  the code
  - The goal of the style guide, code writing guide, linter is exactly to push us
    to write code in a consistent way so that it's less painful to read

- Maybe there are constraints and specs that were not properly documented
- Maybe there were multiple iterations and compromise between different solutions
- Maybe several people were involved
- Maybe a hack solution needed to be added in order to ship and get the $1m from
  the customers

## Suggestions on how to read code
- Use `git blame` to understand who wrote the code and over what period of time
  - Knowing the author can help you ask questions

- Use the same approach as for a code review

- Budget some time, like a few hours and stick to the process for that amount of
  time
  - Otherwise after 5 mins you are like "Argh! I can't do this! I am done"

- Read the specs
  - What is the code supposed to do?
  - What are the edge cases to handle
  - How should it be integrated in the rest of the code

- Read the entire code top to bottom without reading line-by-line
  - Understand the structure
  - What's the goal?
  - What are the specs?
  - What are the functions
  - What is the interface

- Read the unit tests if present

- Run the code with debug output `-v DEBUG` on simple examples to see what it
  does

- Add comments, when missing, to functions, to chunks of code, to each file
  - Watch out for confusing comments
  - Sometimes comment can be out of date

- Add TODOs in the code for yourself

- Use Pycharm to navigate the code and jump around

- Remember the coding convention
  - Global variables are capital letters
  - Functions starting with `_` are just for internals
  - We have all the conventions to convey information about the thought process

- Feel free to take notes about the code
  - Write down the questions about what you don't understand
    - Maybe you will find the answer later (feel free to congratulate yourself!)
    - Send an email to the author with the questions

- Approach reading code as an active form
  - Start writing unit tests for each piece that is not unit test
  - Step through the code with Pycharm

- Factor out the code
  - Make sure you have plenty of unit tests before touching the code

- Expect to find garbage

- Don't feel bad when you get lost
  - Reading code is not linear, like reading a book
  - Rather it's about building a mental model of how the code is structured, how
    it works and why it's done in a certain way

- The more code you read, the easier you will become

## 
- The first thing that programmers want to do is to bulldoze the place flat and
  build something great

- Nobody is excited about incremental renovation
  - improving
  - refactoring
  - cleaning out
  - adding unit tests

- In reality 99.9% of work is incremental

- When you think "the old code is a mess", you are probably wrong

- It's harder to read code than to write it

- For this reason code reuse is hard
- This is why everybody on the team has the same function to do the same thing
- It's easier and more fun to write new code than figuring out how the old code
  works

- The idea that new code is better than old is absurd
  - Old code has been used
  - It has been tested
  - Lots of bugs have been found and fixed

- When code looks a mess is because it handles a lot of corner cases you didn't
  even think about, you didn't even know were possible

- Each of that bug took long time to be discovered and fixed it

- When you throw away code and start from scratch you are throwing away all the
  knowledge, all the bug fixes, all the hard thinking

- What makes the code a "mess", at least according to your very expert opinion:
1) Architectural problems, e.g.,
- Code is not split in pieces in a way that makes sense
- Interfaces that are too broad and brittle

- These problems can be easily fixed!

2) Inefficiency
- Profile and find what is the problem and fix that

3) Ugly
- E.g., horrible variable and function names
- It takes 5 minutes of search and replace

- In other terms, there is no reason to believe that you are going to do a better
  job than others did
